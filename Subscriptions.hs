-- |The Subscriptions module deals with managing subscriptions on the STOMP broker.
module Subscriptions (
    ClientId,
    Response(..),
    SubscriptionManager,
    clientDisconnected,
    initManager,
    unsubscribe,
    subscribe,
    sendAckResponse,
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
--  efficient removal of client subscriptions, as a client UNSUBSCRIBE frame only contains the
--  SubscriptionId, not the Destination.
type ClientDests         = HashMap ClientId (HashMap SubscriptionId Destination)

type AckResponseMap      = HashMap MessageId ClientAckResponse

type AckContextMap       = HashMap MessageId AckContext

-- |ClientAcks stores information about pending ACK situations for individual clients.
type ClientAcks          = HashMap ClientId (AckContextMap, AckResponseMap)

data Subscriptions       = Subscriptions SubMap ClientDests

-- |A ClientSub encapsulates information about a client subscription.
data ClientSub           = ClientSub ClientId SubscriptionId AckType FrameHandler

-- |An AckContext contains all the context surrounding a pending ACK request, including the original
-- Frame in the case that it needs to be resent. The list of ClientIds represents those clients who
-- have already NACK'd the message.
data AckContext          = AckContext Frame MessageId ClientId AckType [ClientId]

data ClientAckResponse   = ClientAckResponse ClientId MessageId Frame

data AckUpdate           = Response ClientAckResponse | Context AckContext | ClientDisconnected ClientId

data UpdateType          = Add Destination ClientSub |
                           Remove ClientId SubscriptionId |
                           GotMessage Destination Frame |
                           ResendMessage Destination Frame [ClientId] MessageId |
                           Ack ClientAckResponse |
                           Disconnected ClientId

data Update              = Update UpdateType (SChan Response)

data Response            = Success (Maybe String) | Error String

data SubscriptionManager = SubscriptionManager (SChan Update)

getSubClientId :: ClientSub -> ClientId
getSubClientId (ClientSub id _ _ _ ) = id

getSubAckType :: ClientSub -> AckType
getSubAckType (ClientSub _ _ ack _) = ack

insertSentClient :: ClientId -> AckContext -> AckContext
insertSentClient clientId (AckContext f m c a ids) = (AckContext f m c a (clientId:ids))

getResponseCommand :: ClientAckResponse -> Command
getResponseCommand (ClientAckResponse _ _ f) = getCommand f

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
sendMessage manager@(SubscriptionManager updateChan) destination frame = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (GotMessage destination frame) responseChan
    sync $ recvEvt responseChan

sendAckResponse :: SubscriptionManager -> ClientId -> Frame -> IO Response
sendAckResponse (SubscriptionManager updateChan) clientId frame = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (Ack $ ClientAckResponse clientId (_getId frame) frame) responseChan
    return $ Success Nothing

clientDisconnected :: SubscriptionManager -> ClientId -> IO Response
clientDisconnected (SubscriptionManager updateChan) clientId = do
    responseChan <- sync newSChan
    sync $ sendEvt updateChan $ Update (Disconnected clientId) responseChan
    return $ Success Nothing

ackLoop :: SChan AckUpdate -> SChan Update -> ClientAcks -> IO ()
ackLoop ackChan updateChan clientAcks = do
    update      <- sync $ recvEvt ackChan
    clientAcks' <- handleAck update clientAcks updateChan
    ackLoop ackChan updateChan clientAcks'

handleAck :: AckUpdate -> ClientAcks -> SChan Update -> IO ClientAcks
handleAck ackUpdate clientAcks updateChan = case ackUpdate of
    Response clientAckResponse  -> handleClientAckResponse clientAckResponse clientAcks updateChan
    Context ackContext          -> handleAckContext ackContext clientAcks updateChan
    ClientDisconnected clientId -> handleClientDisconnect clientAcks clientId updateChan

handleClientAckResponse :: ClientAckResponse -> ClientAcks -> SChan Update -> IO ClientAcks
handleClientAckResponse response@(ClientAckResponse clientId msgId frame) clientAcks updateChan = 
    case HM.lookup clientId clientAcks of
        Just (ackMap, responseMap) -> case HM.lookup msgId ackMap of
            Just context -> handleAckPair context (getResponseCommand response) clientAcks updateChan ackMap responseMap
            Nothing -> return $ HM.insert clientId (ackMap, HM.insert msgId  response responseMap) clientAcks
        Nothing -> return $ HM.insert clientId (HM.empty, HM.singleton msgId response) clientAcks

handleAckContext :: AckContext -> ClientAcks -> SChan Update -> IO ClientAcks
handleAckContext context@(AckContext frame msgId clientId ackType sentClients) clientAcks updateChan =
    case HM.lookup clientId clientAcks of
        Just (ackMap, responseMap) -> case HM.lookup msgId responseMap of
            Just response -> handleAckPair context (getResponseCommand response) clientAcks updateChan ackMap responseMap
            Nothing -> return $ HM.insert clientId (HM.insert msgId context ackMap, responseMap) clientAcks
        Nothing -> return $ HM.insert clientId (HM.singleton msgId context, HM.empty) clientAcks

handleClientDisconnect :: ClientAcks -> ClientId -> SChan Update -> IO ClientAcks
handleClientDisconnect clientAcks clientId updateChan = case HM.lookup clientId clientAcks of
    Just (contextMap, _) -> do
        mapM_ (resendContext updateChan) contextMap
        return $ HM.delete clientId clientAcks
    Nothing              -> return clientAcks

resendContext :: SChan Update -> AckContext -> IO ThreadId
resendContext updateChan ackContext = forkIO $ resendContextFrame updateChan ackContext

handleAckPair :: AckContext -> Command -> ClientAcks -> SChan Update -> AckContextMap -> AckResponseMap -> IO ClientAcks
handleAckPair context@(AckContext frame msgId clId ackType sentClients) cmd clientAcks updateChan ackMap responseMap = do
    case cmd of 
        ACK  -> return ()
        NACK ->
            case ackType of
                Client           -> do { forkIO $ handleNack (elems ackMap) updateChan ; return () }
                ClientIndividual -> do { forkIO $ handleNack [context] updateChan ; return () }
    case ackType of
        Client           -> return $ HM.delete clId clientAcks
        ClientIndividual -> return $ HM.insert clId (HM.delete msgId ackMap, HM.delete msgId responseMap) clientAcks

handleNack :: [AckContext] -> SChan Update -> IO ()
handleNack contexts updateChan = mapM_ (resendContextFrame updateChan) contexts

resendContextFrame :: SChan Update -> AckContext -> IO ()
resendContextFrame updateChan (AckContext frame msgId _ _ sentClients) = do
    responseChan <- sync newSChan
    forkIO $ sync $ sendEvt updateChan (Update (ResendMessage (_getDestination frame) frame sentClients msgId) responseChan)
    sync $ recvEvt responseChan
    return ()

resendTimedOutFrame :: Frame -> SChan Update -> Destination -> [ClientId] -> MessageId -> IO ()
resendTimedOutFrame frame updateChan dest sentClients messageId = do
    responseChan <- sync newSChan
    forkIO $ sync $ sendEvt updateChan (Update (ResendMessage dest frame sentClients messageId) responseChan)
    sync $ recvEvt responseChan
    return ()

updateLoop :: SChan Update -> SChan AckUpdate -> Subscriptions -> IO ()
updateLoop updateChan ackChan subs = do
    update  <- sync $ recvEvt updateChan
    subs' <- handleUpdate update subs updateChan ackChan
    updateLoop updateChan ackChan subs'

handleUpdate :: Update -> Subscriptions -> SChan Update -> SChan AckUpdate -> IO Subscriptions
-- Add
handleUpdate (Update (Add dest clientSub) rChan) subscriptions _ _ = do
    forkIO $ sync $ sendEvt rChan (Success Nothing)
    return $ addSubscription dest clientSub subscriptions
-- Remove
handleUpdate (Update (Remove clientId subId) rChan) subs _ _ = do
    forkIO $ sync $ sendEvt rChan (Success Nothing)
    return $ removeSubscription clientId subId subs
-- GotMessage
handleUpdate (Update (GotMessage dest frame) rChan) subs updateChan ackChan = do
    forkIO $ handleMessage frame dest subs [] Nothing rChan ackChan updateChan
    return subs
-- Resend message due to NACK response
handleUpdate (Update (ResendMessage dest frame sentClients messageId) rChan) subs updateChan ackChan = do
    forkIO $ handleMessage frame dest subs sentClients (Just messageId) rChan ackChan updateChan
    return subs
-- Ack response from client
handleUpdate (Update (Ack clientResponse) rChan) subs _ ackChan = do
    forkIO $ sync $ sendEvt ackChan (Response clientResponse)
    return subs
-- Client disconnected
handleUpdate (Update (Disconnected clientId) rChan) subs@(Subscriptions subMap clientDests) _ ackChan = do
    forkIO $ sync $ sendEvt ackChan (ClientDisconnected clientId)
    return $ removeAllClientSubs subs clientId

removeAllClientSubs :: Subscriptions -> ClientId -> Subscriptions
removeAllClientSubs subs@(Subscriptions subMap clientDests) clientId = case HM.lookup clientId clientDests of
    Just destMap -> Subscriptions (deleteSubs subMap clientId (elems destMap)) (HM.delete clientId clientDests)
    Nothing      -> subs

deleteSubs :: SubMap -> ClientId -> [Destination] -> SubMap
deleteSubs subMap _ []                 = subMap
deleteSubs subMap clientId (dest:rest) = case HM.lookup dest subMap of
    Just clientSubs -> deleteSubs (HM.insert dest (HM.delete clientId clientSubs) subMap) clientId rest
    Nothing         -> deleteSubs subMap clientId rest

addSubscription :: Destination -> ClientSub -> Subscriptions -> Subscriptions
addSubscription dest clientSub@(ClientSub clientId subId _ _) (Subscriptions subMap clientDests) =
    let clientSubs' = case HM.lookup dest subMap of
            Just clientSubs -> HM.insert clientId clientSub clientSubs
            Nothing         -> HM.singleton clientId clientSub
        destMap'    = case HM.lookup clientId clientDests of
            Just destMap    -> HM.insert subId dest destMap
            Nothing         -> HM.singleton subId dest
    in
        Subscriptions (insert dest clientSubs' subMap) (insert clientId destMap' clientDests)

removeSubscription :: ClientId -> SubscriptionId -> Subscriptions -> Subscriptions
removeSubscription clientId subId subs@(Subscriptions subMap clientDests) =
    case HM.lookup clientId clientDests of
        Just destMap ->
            let clientDests' = HM.insert clientId (HM.delete subId destMap) clientDests
            in case HM.lookup subId destMap of 
                Just dest -> case HM.lookup dest subMap of
                    Just clientSubs -> Subscriptions (HM.insert dest (HM.delete clientId clientSubs) subMap) clientDests'
                    Nothing         -> Subscriptions subMap clientDests'
                Nothing          -> Subscriptions subMap clientDests'
        Nothing -> subs

handleMessage :: Frame -> Destination -> Subscriptions -> [ClientId] -> Maybe MessageId -> SChan Response -> SChan AckUpdate -> SChan Update -> IO ()
handleMessage frame dest subs@(Subscriptions subMap _) sentClients maybeId responseChan ackChan  updateChan =
    case HM.lookup dest subMap of
        Just clientSubs -> do
            sync $ sendEvt responseChan (Success Nothing)
            messageId    <- getNewMessageId maybeId
            frame'       <- return $ addFrameHeaderFront (messageIdHeader messageId) frame
            maybeSub     <- sync $ clientChoiceEvt frame' messageId sentClients clientSubs
            case maybeSub of
                Just clientSub -> do
                    clientId     <- return $ getSubClientId clientSub
                    context      <- return $ AckContext frame messageId clientId (getSubAckType clientSub) (clientId:sentClients)
                    case (getSubAckType clientSub) of
                        Auto -> return ()
                        _    -> do
                            forkIO $ sync $ sendEvt ackChan (Context context)
                            return ()
                    sync $ sendEvt responseChan $ Success Nothing
                Nothing -> do { forkIO $ resendTimedOutFrame frame updateChan dest sentClients messageId ; return () }
        Nothing -> sync $ sendEvt responseChan (Error "No subscribers")

getNewMessageId :: Maybe MessageId -> IO MessageId
getNewMessageId maybeId = case maybeId of
    Just messageId -> return messageId
    Nothing        -> do
        unique <- newUnique
        return $ show $ hashUnique unique

clientChoiceEvt :: Frame -> MessageId -> [ClientId] -> HashMap ClientId ClientSub -> Evt (Maybe ClientSub)
clientChoiceEvt frame messageId sentClients = HM.foldr (partialClientChoiceEvt frame messageId sentClients) 
    $ (timeOutEvt 500000) `thenEvt` (\_ -> alwaysEvt Nothing)

partialClientChoiceEvt :: Frame -> MessageId -> [ClientId] -> ClientSub -> Evt (Maybe ClientSub) -> Evt (Maybe ClientSub)
partialClientChoiceEvt frame messageId sentClients sub@(ClientSub clientId _ _ frameHandler) = 
    if clientId `elem` sentClients then (chooseEvt neverEvt) else let frame' = transformFrame frame messageId sub in
        chooseEvt $ (putEvt frame' frameHandler) `thenEvt` (\_ -> alwaysEvt $ Just sub)

transformFrame :: Frame -> MessageId -> ClientSub -> Frame
transformFrame (Frame _ headers body) messageId (ClientSub _ subId ackType _) = 
    let frame = Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body in
        case ackType of 
            Auto -> frame
            _    -> addFrameHeaderFront (ackHeader messageId) frame

