-- |The Subscriptions module deals with managing subscriptions on the STOMP broker.
module Subscriptions (
    ClientId,
    Response(..),
    SubscriptionManager,
    initManager,
    unsubscribe,
    subscribe,
    sendMessage
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Data.HashMap.Strict as HM
import Stomp.Frames hiding (subscribe)
import Stomp.Frames.IO

type ClientId            = Int
type SubscriptionId      = String
type Destination         = String

-- |A HashMap to keep track of subscriptions to a given Destination.
type SubMap              = HashMap Destination (HashMap ClientId ClientSub)

-- |A HashMap to track which ClientId/SubscriptionId pairs are on which Destination. Allows for
--  efficient removal of client subscriptions, as a cli9ent UNSUBSCRIBE frame only contains the
--  SubscriptionId, not the Destination.
type SubIds              = HashMap (ClientId, SubscriptionId) Destination

data Subscriptions       = Subscriptions SubMap SubIds

-- |A ClientSub encapsulates information about a client subscription.
data ClientSub           = ClientSub ClientId SubscriptionId FrameHandler

data UpdateType          = Add Destination ClientSub |
                           Remove ClientId SubscriptionId |
                           GotMessage Destination Frame

data Update              = Update UpdateType (SChan Response)

data Response            = Success | Error String

data SubscriptionManager = SubscriptionManager (SChan Update)

initManager :: IO SubscriptionManager
initManager = do
    updateChan    <- sync newSChan
    responseChan  <- sync newSChan
    subscriptions <- return $ Subscriptions HM.empty HM.empty
    forkIO $ updateLoop updateChan subscriptions
    return $ SubscriptionManager updateChan

subscribe :: SubscriptionManager -> Destination -> ClientId -> SubscriptionId -> FrameHandler -> IO Response
subscribe (SubscriptionManager updateChan) destination clientId subId frameHandler = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (Add destination $ ClientSub clientId subId frameHandler) responseChan
    sync $ recvEvt responseChan

unsubscribe :: SubscriptionManager -> ClientId -> SubscriptionId -> IO Response
unsubscribe (SubscriptionManager updateChan) clientId subId = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (Remove clientId subId) responseChan
    sync $ recvEvt responseChan

sendMessage :: SubscriptionManager -> Destination -> Frame -> IO Response
sendMessage (SubscriptionManager updateChan) destination frame = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (GotMessage destination frame) responseChan
    sync $ recvEvt responseChan


updateLoop :: SChan Update -> Subscriptions -> IO ()
updateLoop updateChan subs = do
    update  <- sync $ recvEvt updateChan
    subs' <- handleUpdate update subs
    updateLoop updateChan subs'

handleUpdate :: Update -> Subscriptions -> IO Subscriptions
-- Add
handleUpdate (Update (Add dest clientSub) rChan) subscriptions = do
    forkIO $ sync $ sendEvt rChan Success
    return $ addSubscription dest clientSub subscriptions
-- Remove
handleUpdate (Update (Remove clientId subId) rChan) subs = do
    forkIO $ sync $ sendEvt rChan Success
    return $ removeSubscription clientId subId subs
-- GotMessage
handleUpdate (Update (GotMessage dest frame) rChan) subs = do
    forkIO $ handleMessage frame dest subs rChan
    return subs

addSubscription :: Destination -> ClientSub -> Subscriptions -> Subscriptions
addSubscription dest clientSub@(ClientSub clientId subId _) (Subscriptions subMap subIds) =
    let clientSubs' = case HM.lookup dest subMap of
            Just clientSubs -> HM.insert clientId clientSub clientSubs
            Nothing         -> HM.singleton clientId clientSub
    in
        Subscriptions (insert dest clientSubs' subMap) (insert (clientId, subId) dest subIds)

removeSubscription :: ClientId -> SubscriptionId -> Subscriptions -> Subscriptions
removeSubscription clientId subId subs@(Subscriptions subMap subIds) =
    case HM.lookup (clientId, subId) subIds of
        Just destination -> let subIds' = HM.delete (clientId, subId) subIds in
            case HM.lookup destination subMap of
                Just clients -> Subscriptions (HM.insert destination (HM.delete clientId clients) subMap) subIds'
                Nothing      -> Subscriptions subMap subIds'
        Nothing          -> subs

handleMessage :: Frame -> Destination -> Subscriptions -> SChan Response -> IO ()
handleMessage frame dest (Subscriptions subMap _) responseChan =
    case HM.lookup dest subMap of
        Just clientSubs -> do
            sync $ constructEvt frame clientSubs
            sync $ sendEvt responseChan Success
        Nothing -> sync $ sendEvt responseChan (Error "No subscribers") 

constructEvt :: Frame -> HashMap ClientId ClientSub -> Evt ()
constructEvt frame = HM.foldr (partialEvt frame) neverEvt

partialEvt :: Frame -> ClientSub -> Evt () -> Evt ()
partialEvt frame sub@(ClientSub _ _ frameHandler) = 
    let frame' = transformFrame frame sub in
        chooseEvt (putEvt frame' frameHandler)

transformFrame :: Frame -> ClientSub -> Frame
transformFrame (Frame _ headers body) (ClientSub _ subId _) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body
