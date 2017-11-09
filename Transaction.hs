module Transaction (

) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Data.HashMap.Strict as HM
import Stomp.Frames
import Subscriptions

type TransactionMap = HashMap String Transaction
type TransactionId  = String
type Transaction    = Evt ()

data TransactionUpdate = Begin TransactionId | 
                         Commit TransactionId | 
                         Abort TransactionId | 
                         AckResponse

data UpdateResponse    = Success | Error String

-- transactionLoop :: TransactionMap -> SChan TransactionUpdate -> SChan Update -> IO ()
-- transactionLoop transactionMap transactionChan subUpdateChan = do
--  transactionMap' <- sync $ (recvEvt transactionChan `thenEvt` updateChan

-- updateEvt :: TransactionMap -> SChan TransactionUpdate -> SChan TransactionResponse -> Evt TransactionMap
-- updateEvt transactionMap transactionChan responseChan =
--  recvEvt (transactionChan `thenEvt` (\update -> alwaysEvt $ handleUpdate update transactionMap)) 
--      `thenEvt` (\(tMap', response) -> (sendEvt responseChan response) `thenEvt` (alwaysEvt )


updateEvt :: TransactionMap -> SChan TransactionUpdate -> SChan UpdateResponse -> SubscriptionManager -> Evt TransactionMap
updateEvt transactionMap transactionChan responseChan subManager = do
    update            <- recvEvt transactionChan
    (tmap', response) <- handleUpdate update transactionMap subManager
    sendEvt responseChan response
    alwaysEvt tmap'

handleUpdate :: TransactionUpdate -> TransactionMap -> SubscriptionManager -> Evt (TransactionMap, UpdateResponse)
handleUpdate update tMap = case update of
    Begin transactionId  -> case HM.lookup transactionId tMap of
        Nothing -> alwaysEvt (HM.insert transactionId (alwaysEvt ()) tMap, Success)
        _       -> alwaysEvt (tMap, Error $ "Attempt to begin an existing transaction: " ++ transactionId)
    Commit transactionId -> case HM.lookup transactionId tMap of
        Just transaction -> transaction `thenEvt` (\_ -> alwaysEvt ((HM.delete transactionId tMap), Success))
        Nothing          -> alwaysEvt (tMap, Error $ "Attempt to commit a non-existant transaction: " ++ transactionId)
    Abort transactionId     -> alwaysEvt (HM.delete transactionId tMap, Success)
    Add transactionId frame -> case HM.lookup transactionId tMap of
        Just transaction -> alwaysEvt (HM.insert transactionId (transaction `thenEvt` (\_ -> frameToEvt frame)) tMap, Success)
        Nothing          -> alwaysEvt (tMap, Error $ "Attempt to add to a non-existant transaction: " ++ transactionId)

frameToEvt :: Frame -> SubscriptionManager -> Evt ()
frameToEvt frame subManager = case getCommand frame of
    SEND -> 

