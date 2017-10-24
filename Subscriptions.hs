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
import Data.Unique (hashUnique, newUnique)
import Stomp.Frames hiding (subscribe)
import Stomp.Frames.IO

type ClientId            = Int
type MessageId           = String
type SubscriptionId      = String
type Destination         = String

-- |A HashMap to keep track of subscriptions to a given Destination.
type SubMap              = HashMap Destination (HashMap ClientId ClientSub)

-- |A HashMap to track which ClientId/SubscriptionId pairs are on which Destination. Allows for
--  efficient removal of client subscriptions, as a cli9ent UNSUBSCRIBE frame only contains the
--  SubscriptionId, not the Destination.
type SubIds              = HashMap (ClientId, SubscriptionId) Destination


-- |The AckMap stores either an AckContext or a Frame (should be a NACK or ACK Frame). This is to
-- avoid a race condition in whihc an ACK/NACK Frame could be received prior to the NeedsAck Update.
type AckMap              = HashMap MessageId (Either Frame AckContext)

-- |ClientAcks stores information about pending ACK situations for individual clients.
type ClientAcks          = HashMap ClientId AckMap

data Subscriptions       = Subscriptions SubMap SubIds

-- |A ClientSub encapsulates information about a client subscription.
data ClientSub           = ClientSub ClientId SubscriptionId AckType FrameHandler

-- |An AckContext contains all the context surrounding a pending ACK request, including the original
-- Frame in the case that it needs to be resent. The list of ClientIds represents those clients who
-- have already NACK'd the message.
data AckContext          = AckContext Frame ClientSub [ClientId]

data UpdateType          = Add Destination ClientSub |
                           Remove ClientId SubscriptionId |
                           GotMessage Destination Frame |
                           AckUpdate (Either Frame AckContext)

data Update              = Update UpdateType (SChan Response)

data Response            = Success | Error String

data SubscriptionManager = SubscriptionManager (SChan Update)

initManager :: IO SubscriptionManager
initManager = do
    updateChan    <- sync newSChan
    responseChan  <- sync newSChan
    ackChan       <- sync newSChan
    subscriptions <- return $ Subscriptions HM.empty HM.empty
    forkIO $ updateLoop updateChan ackChan subscriptions 
    forkIO $ ackLoop ackChan updateChan HM.empty
    return $ SubscriptionManager updateChan

subscribe :: SubscriptionManager -> Destination -> ClientId -> SubscriptionId -> AckType -> FrameHandler -> IO Response
subscribe (SubscriptionManager updateChan) destination clientId subId ackType frameHandler = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (Add destination $ ClientSub clientId subId ackType frameHandler) responseChan
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

ackLoop :: SChan (Either Frame AckContext) -> SChan Update -> ClientAcks -> IO ()
ackLoop ackChan updateChan clientAcks = do
    ackData     <- sync $ recvEvt ackChan
    clientAcks' <- handleAck ackData clientAcks
    ackLoop ackChan updateChan clientAcks'

handleAck :: Either Frame AckContext -> ClientAcks -> IO ClientAcks
handleAck ackData clientAcks = return clientAcks

updateLoop :: SChan Update -> SChan (Either Frame AckContext) -> Subscriptions -> IO ()
updateLoop updateChan ackChan subs = do
    update  <- sync $ recvEvt updateChan
    subs' <- handleUpdate update subs updateChan ackChan
    updateLoop updateChan ackChan subs'

handleUpdate :: Update -> Subscriptions -> SChan Update -> SChan (Either Frame AckContext) -> IO Subscriptions
-- Add
handleUpdate (Update (Add dest clientSub) rChan) subscriptions _ _ = do
    forkIO $ sync $ sendEvt rChan Success
    return $ addSubscription dest clientSub subscriptions
-- Remove
handleUpdate (Update (Remove clientId subId) rChan) subs _ _ = do
    forkIO $ sync $ sendEvt rChan Success
    return $ removeSubscription clientId subId subs
-- GotMessage
handleUpdate (Update (GotMessage dest frame) rChan) subs _ ackChan = do
    forkIO $ handleMessage frame dest subs rChan ackChan
    return subs
handleUpdate (Update (AckUpdate ackData) rChan) subs _ ackChan = do
    forkIO $ sync $ sendEvt ackChan ackData
    return subs

addSubscription :: Destination -> ClientSub -> Subscriptions -> Subscriptions
addSubscription dest clientSub@(ClientSub clientId subId _ _) (Subscriptions subMap subIds) =
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

handleMessage :: Frame -> Destination -> Subscriptions -> SChan Response -> SChan (Either Frame AckContext) -> IO ()
handleMessage frame dest (Subscriptions subMap _) responseChan ackChan =
    case HM.lookup dest subMap of
        Just clientSubs -> do
            unique <- newUnique
            frame'  <- return $ addFrameHeaderFront (messageIdHeader $ show $ hashUnique unique) frame
            forkIO $ sync $ sendEvt responseChan Success
            clientSub <- sync $ clientChoiceEvt frame' clientSubs
            forkIO $ sync $ sendEvt ackChan (Right (AckContext frame' clientSub []))
            return ()
        Nothing -> sync $ sendEvt responseChan (Error "No subscribers") 

clientChoiceEvt :: Frame -> HashMap ClientId ClientSub -> Evt ClientSub
clientChoiceEvt frame = HM.foldr (partialClientChoiceEvt frame) neverEvt

partialClientChoiceEvt :: Frame -> ClientSub -> Evt ClientSub -> Evt ClientSub
partialClientChoiceEvt frame sub@(ClientSub _ _ _ frameHandler) = 
    let frame' = transformFrame frame sub in
        chooseEvt $ (putEvt frame' frameHandler) `thenEvt` (\_ -> alwaysEvt sub)

transformFrame :: Frame -> ClientSub -> Frame
transformFrame (Frame _ headers body) (ClientSub _ subId _ _) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body
