-- |The Stomp.Transaction module implements a ClientTransactionManager that can be used,
-- in conjunction with a SubscriptionManager, to handle STOMP transactiosn for a single client.
module Stomp.Transaction (
    ClientTransactionManager,
    UpdateResponse(..),
    TransactionId,
    initTransactionManager,
    begin,
    commit,
    abort,
    ackResponse,
    send,
    disconnect
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Data.HashMap.Strict as HM
import Stomp.Frames hiding (disconnect, commit, begin, abort)
import Stomp.Subscriptions

-- |A TransactionId is a unique (per client) transaction identifier
type TransactionId      = String

-- |A TransactionMap maps TransactionIds to Transactions
type TransactionMap     = HashMap TransactionId Transaction

-- |A Transaction is a composite event that is synchronized on upon commit
type Transaction        = Evt ()

-- |TransactionUpdates represent the different actions that can be taken within a STOMP
-- transaction.
data TransactionUpdate  = Begin TransactionId | 
                          Commit TransactionId | 
                          Abort TransactionId | 
                          AckResponse TransactionId ClientId Frame |
                          Send TransactionId Destination Frame |
                          ClientDisconnect

-- |An update will either be successful or generate an error message.
data UpdateResponse           = Success | Error String

-- |Encapuslates the communications channels for a client transaction manager.
data ClientTransactionManager = Manager (SChan TransactionUpdate) (SChan UpdateResponse)

-- |Initialize a ClientTransactionManager and return it in an IO context.
initTransactionManager :: SubscriptionManager -> IO ClientTransactionManager
initTransactionManager subManager = do
    updateChan   <- sync newSChan
    responseChan <- sync newSChan
    forkIO $ transactionLoop HM.empty updateChan responseChan subManager
    return $ Manager updateChan responseChan

-- |Begin a Transaction with the given TransactionId
begin :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
begin transactionId manager = sendUpdate (Begin transactionId) manager

-- |Commit the Transaction with the given TransactionId
commit :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
commit transactionId manager = sendUpdate (Commit transactionId) manager

-- |Abort the Transaction with the given TransactionId
abort :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
abort transactionId manager = sendUpdate (Abort transactionId) manager

-- |Add an AckResponse to the Transaction with the given TransactionId
ackResponse :: TransactionId -> ClientId -> Frame -> ClientTransactionManager -> IO UpdateResponse
ackResponse transactionId clientId frame manager = sendUpdate (AckResponse transactionId clientId frame) manager

-- |Add a SEND frame to the Transaction with the given TransactionId
send :: TransactionId -> Destination -> Frame -> ClientTransactionManager -> IO UpdateResponse
send transactionId dest frame manager = sendUpdate (Send transactionId dest frame) manager

-- |Send a "disconnect" notice; all pending transactions will be aborted. The ClientTransactionManager
-- should not be used after calling this function.
disconnect :: ClientTransactionManager -> IO UpdateResponse
disconnect manager = sendUpdate ClientDisconnect manager

-- |Helper function to synchronize on an update.
sendUpdate :: TransactionUpdate -> ClientTransactionManager -> IO UpdateResponse
sendUpdate update (Manager updateChan responseChan) =
    let event = do
            sendEvt updateChan update
            recvEvt responseChan
    in sync event

-- |State management loop for an individual client's transactions.
transactionLoop :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> IO ()
transactionLoop tmap updateChan responseChan subManager = do
    mapUpdate <- sync $ updateEvt tmap updateChan responseChan subManager
    case mapUpdate of
        Just tmap' -> transactionLoop tmap' updateChan responseChan subManager
        Nothing    -> return ()

-- |Given a TransactionMap and TransactionUpdate channel, wait to receive an update and return the 
-- updated state of the TransactionMap in the Evt context.
updateEvt :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> Evt (Maybe TransactionMap)
updateEvt transactionMap transactionChan responseChan subManager = do
    update            <- recvEvt transactionChan
    case update of
        ClientDisconnect -> do
            sendEvt responseChan Success
            alwaysEvt Nothing
        otherwise        -> do 
            (tmap', response) <- handleUpdate update transactionMap subManager
            sendEvt responseChan response
            alwaysEvt $ Just tmap'

-- |This function contains the logic for handling the respective update types and modifying the TransactionMap.
handleUpdate :: TransactionUpdate -> TransactionMap -> SubscriptionManager -> Evt (TransactionMap, UpdateResponse)
handleUpdate update tmap subManager = case update of
    Begin transactionId  -> case HM.lookup transactionId tmap of
        Nothing -> alwaysEvt (HM.insert transactionId (alwaysEvt ()) tmap, Success)
        _       -> alwaysEvt (tmap, Error $ "Attempt to begin an existing transaction: " ++ transactionId)
    Commit transactionId -> case HM.lookup transactionId tmap of
        Just transaction -> transaction `thenEvt` (\_ -> alwaysEvt ((HM.delete transactionId tmap), Success))
        Nothing          -> alwaysEvt (tmap, Error $ "Attempt to commit a non-existent transaction: " ++ transactionId)
    Abort transactionId        -> alwaysEvt (HM.delete transactionId tmap, Success)
    AckResponse transactionId clientId frame -> case HM.lookup transactionId tmap of 
        Just transaction -> 
            alwaysEvt (HM.insert transactionId (transaction `thenEvt` (\_ -> ackResponseEvt subManager clientId frame)) tmap, Success)
        Nothing          -> 
            alwaysEvt (tmap, Error $ "Attempt to add to a non-existent transaction: " ++ transactionId)
    Send transactionId dest frame     -> case HM.lookup transactionId tmap of
        Just transaction -> 
            alwaysEvt (HM.insert transactionId (transaction `thenEvt` (\_ -> sendMessageEvt subManager dest frame)) tmap, Success)
        Nothing ->
            alwaysEvt (tmap, Error $ "Attempt to add to a non-existent transaction: " ++ transactionId)
