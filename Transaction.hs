module Transaction (
    ClientTransactionManager,
    initTransactionManager,
    begin,
    commit,
    abort,
    ackResponse,
    send
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Data.HashMap.Strict as HM
import Stomp.Frames
import Subscriptions

type TransactionMap = HashMap String Transaction
type TransactionId  = String
type Transaction    = Evt ()

data TransactionUpdate  = Begin TransactionId | 
                         Commit TransactionId | 
                         Abort TransactionId | 
                         AckResponse TransactionId ClientId Frame |
                         Send TransactionId Destination Frame

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

sendUpdate :: TransactionUpdate -> ClientTransactionManager -> IO UpdateResponse
sendUpdate update (Manager updateChan responseChan) =
    let event = do
        sendEvt updateChan update
        recvEvt responseChan
    in sync event

transactionLoop :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> IO ()
transactionLoop tMap updateChan responseChan subManager = do
    tMap' <- sync $ updateEvt tMap updateChan responseChan subManager
    transactionLoop tMap' updateChan responseChan subManager

updateEvt :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> Evt TransactionMap
updateEvt transactionMap transactionChan responseChan subManager = do
    update            <- recvEvt transactionChan
    (tmap', response) <- handleUpdate update transactionMap subManager
    sendEvt responseChan response
    alwaysEvt tmap'

handleUpdate :: TransactionUpdate -> TransactionMap -> SubscriptionManager -> Evt (TransactionMap, UpdateResponse)
handleUpdate update tMap subManager = case update of
    Begin transactionId  -> case HM.lookup transactionId tMap of
        Nothing -> alwaysEvt (HM.insert transactionId (alwaysEvt ()) tMap, Success)
        _       -> alwaysEvt (tMap, Error $ "Attempt to begin an existing transaction: " ++ transactionId)
    Commit transactionId -> case HM.lookup transactionId tMap of
        Just transaction -> transaction `thenEvt` (\_ -> alwaysEvt ((HM.delete transactionId tMap), Success))
        Nothing          -> alwaysEvt (tMap, Error $ "Attempt to commit a non-existent transaction: " ++ transactionId)
    Abort transactionId        -> alwaysEvt (HM.delete transactionId tMap, Success)
    AckResponse transactionId clientId frame -> case HM.lookup transactionId tMap of 
        Just transaction -> 
            alwaysEvt (HM.insert transactionId (transaction `thenEvt` (\_ -> ackResponseEvt subManager clientId frame)) tMap, Success)
        Nothing          -> 
            alwaysEvt (tMap, Error $ "Attempt to add to a non-existent transaction: " ++ transactionId)
    Send transactionId dest frame     -> case HM.lookup transactionId tMap of
        Just transaction -> 
            alwaysEvt (HM.insert transactionId (transaction `thenEvt` (\_ -> sendMessageEvt subManager dest frame)) tMap, Success)
        Nothing ->
            alwaysEvt (tMap, Error $ "Attempt to add to a non-existent transaction: " ++ transactionId)
