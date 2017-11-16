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

type TransactionMap = HashMap String Transaction
type TransactionId  = String
type Transaction    = Evt ()

data TransactionUpdate  = Begin TransactionId | 
                         Commit TransactionId | 
                         Abort TransactionId | 
                         AckResponse TransactionId ClientId Frame |
                         Send TransactionId Destination Frame |
                         ClientDisconnect

data UpdateResponse           = Success | Error String

data ClientTransactionManager = Manager (SChan TransactionUpdate) (SChan UpdateResponse)


initTransactionManager :: SubscriptionManager -> IO ClientTransactionManager
initTransactionManager subManager = do
    updateChan   <- sync newSChan
    responseChan <- sync newSChan
    forkIO $ transactionLoop HM.empty updateChan responseChan subManager
    return $ Manager updateChan responseChan

begin :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
begin transactionId manager = sendUpdate (Begin transactionId) manager

commit :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
commit transactionId manager = sendUpdate (Commit transactionId) manager

abort :: TransactionId -> ClientTransactionManager -> IO UpdateResponse
abort transactionId manager = sendUpdate (Abort transactionId) manager

ackResponse :: TransactionId -> ClientId -> Frame -> ClientTransactionManager -> IO UpdateResponse
ackResponse transactionId clientId frame manager = sendUpdate (AckResponse transactionId clientId frame) manager

send :: TransactionId -> Destination -> Frame -> ClientTransactionManager -> IO UpdateResponse
send transactionId dest frame manager = sendUpdate (Send transactionId dest frame) manager

disconnect :: ClientTransactionManager -> IO UpdateResponse
disconnect manager = sendUpdate ClientDisconnect manager

sendUpdate :: TransactionUpdate -> ClientTransactionManager -> IO UpdateResponse
sendUpdate update (Manager updateChan responseChan) =
    let event = do
            sendEvt updateChan update
            recvEvt responseChan
    in sync event

transactionLoop :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> IO ()
transactionLoop tmap updateChan responseChan subManager = do
    mapUpdate <- sync $ updateEvt tmap updateChan responseChan subManager
    case mapUpdate of
        Just tmap' -> transactionLoop tmap' updateChan responseChan subManager
        Nothing    -> return ()

updateEvt :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> Evt (Maybe TransactionMap)
updateEvt transactionMap transactionChan responseChan subManager = do
    update            <- recvEvt transactionChan
    case update of
        ClientDisconnect -> do
            sendEvt responseChan Success
            alwaysEvt Nothing
        _                -> do 
            (tmap', response) <- handleUpdate update transactionMap subManager
            sendEvt responseChan response
            alwaysEvt $ Just tmap'

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
