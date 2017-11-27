-- |The Subscriptions module deals with managing subscriptions on the STOMP broker.
module Stomp.Subscriptions (
    ClientId,
    Destination,
    SubscriptionManager,
    clientDisconnected,
    initManager,
    unsubscribe,
    subscribe,
    sendAckResponse,
    ackResponseEvt,
    sendMessage,
    sendMessageEvt
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Data.HashMap.Strict as HM
import Prelude hiding (log)
import Stomp.Frames hiding (subscribe, unsubscribe)
import Stomp.Frames.IO
import Stomp.Increment
import Stomp.TLogger
import System.IO

-- |A unique client identifier
type ClientId            = Integer

-- |A unique message identifier
type MessageId           = Integer

-- |A per-client unique subscription identifier
type SubscriptionId      = String

-- |A unique destination identifier
type Destination         = String

-- |A HashMap to keep track of subscriptions to a given Destination.
type SubMap              = HashMap Destination (HashMap ClientId ClientSub)

-- |A HashMap to track which ClientId/SubscriptionId pairs are on which Destination. Allows for
--  efficient removal of client subscriptions, as a client UNSUBSCRIBE frame only contains the
--  SubscriptionId, not the Destination.
type ClientDests         = HashMap ClientId (HashMap SubscriptionId Destination)

-- |A map of ACK/NACK responses received from clients
type AckResponseMap      = HashMap MessageId ClientAckResponse

-- |A map of AckContexts generated by the server
type AckContextMap       = HashMap MessageId AckContext

-- |ClientAcks stores information about pending ACK situations for individual clients.
type ClientAcks          = HashMap ClientId (AckContextMap, AckResponseMap)

-- |Encapsulation of all subascription data
data Subscriptions       = Subscriptions SubMap ClientDests

-- |A ClientSub encapsulates information about a client subscription.
data ClientSub           = ClientSub ClientId SubscriptionId AckType FrameHandler

-- |An AckContext contains all the context surrounding a pending ACK request, including the original
-- Frame in the case that it needs to be resent. The list of ClientIds represents those clients who
-- have already NACK'd the message.
data AckContext          = AckContext Frame MessageId ClientId AckType [ClientId]

-- |Encapsulation of data surrounding an ACK/NACK response from a client
data ClientAckResponse   = ClientAckResponse ClientId MessageId Frame

-- |Update type for ack management
data AckUpdate           = Response ClientAckResponse | Context AckContext | ClientDisconnected ClientId

-- |General Update type for main processing loop
data Update              = Add Destination ClientSub |
                           Remove ClientId SubscriptionId |
                           GotMessage Destination Frame |
                           ResendMessage Destination Frame [ClientId] MessageId |
                           Ack ClientAckResponse |
                           Disconnected ClientId

-- |The SubscriptionManager allows the server to add and remove new subscriptions, send messages 
-- to destinations, and send ACK/NACK responses.
data SubscriptionManager = SubscriptionManager (SChan Update)

-- |Conveninece function to get the ClientId from a ClientSub
getSubClientId :: ClientSub -> ClientId
getSubClientId (ClientSub id _ _ _ ) = id

-- |Convenience function to get the AckType from a ClientSub
getSubAckType :: ClientSub -> AckType
getSubAckType (ClientSub _ _ ack _) = ack

-- |Conveninece function to insert a sent client into an AckContext
insertSentClient :: ClientId -> AckContext -> AckContext
insertSentClient clientId (AckContext f m c a ids) = (AckContext f m c a (clientId:ids))

-- |Conveninece function to get the Command out of a ClientAckResponse
getResponseCommand :: ClientAckResponse -> Command
getResponseCommand (ClientAckResponse _ _ f) = getCommand f

-- |Initialize a SubscriptionManager and return it in an IO context.
initManager :: IO SubscriptionManager
initManager = do
    updateChan      <- sync newSChan
    lowPriorityChan <- sync newSChan
    ackChan         <- sync newSChan
    incrementer     <- newIncrementer
    initMessageCounter incrementer
    subscriptions <- return $ Subscriptions HM.empty HM.empty
    forkIO $ updateLoop updateChan lowPriorityChan ackChan subscriptions incrementer
    forkIO $ ackLoop ackChan updateChan HM.empty
    return $ SubscriptionManager updateChan

initMessageCounter :: Incrementer -> IO ThreadId
initMessageCounter inc = do
    fileHandle   <- openFile "sentCount.log" AppendMode
    hSetBuffering fileHandle NoBuffering
    logger       <- dateTimeLogger fileHandle
    forkIO $ messageCountLoop inc logger 0

messageCountLoop :: Incrementer -> Logger -> Integer -> IO ()
messageCountLoop inc logger lastCount = do
    currentCount <- sync $ timeOutEvt 1000000 `thenEvt` (\_ -> getLastEvt inc)
    log logger $ show (currentCount - lastCount)
    messageCountLoop inc logger currentCount

-- |Subscribe to a destination; if the destination does not already exist it will be created.
subscribe :: SubscriptionManager -> Destination -> ClientId -> SubscriptionId -> AckType -> FrameHandler -> IO ()
subscribe (SubscriptionManager updateChan) destination clientId subId ackType frameHandler = do
    sync $ sendEvt updateChan $ Add destination $ ClientSub clientId subId ackType frameHandler

-- |Unsubscribe from a destination.
unsubscribe :: SubscriptionManager -> ClientId -> SubscriptionId -> IO ()
unsubscribe (SubscriptionManager updateChan) clientId subId = do
    sync $ sendEvt updateChan $ Remove clientId subId

-- |Send a Frame to a Destination in an IO context.
sendMessage :: SubscriptionManager -> Destination -> Frame -> IO ()
sendMessage manager destination frame = do
    sync $ sendMessageEvt manager destination frame

-- |Send a Frame to a Destination in an Evt context.
sendMessageEvt :: SubscriptionManager -> Destination -> Frame -> Evt ()
sendMessageEvt manager@(SubscriptionManager updateChan) destination frame = 
    sendEvt updateChan $ GotMessage destination frame

-- |Send an ack response in an IO context
sendAckResponse :: SubscriptionManager -> ClientId -> Frame -> IO ()
sendAckResponse manager clientId frame = do
    sync $ ackResponseEvt manager clientId frame

-- |Send an ack response in an Evt context
ackResponseEvt :: SubscriptionManager -> ClientId -> Frame -> Evt ()
ackResponseEvt (SubscriptionManager updateChan) clientId frame = 
    sendEvt updateChan $ Ack $ ClientAckResponse clientId (read (_getId frame)::Integer) frame

-- |Report a client disconnect
clientDisconnected :: SubscriptionManager -> ClientId -> IO ()
clientDisconnected (SubscriptionManager updateChan) clientId = do
    sync $ sendEvt updateChan $ Disconnected clientId

-- |State management loop for ClientAcks
ackLoop :: SChan AckUpdate -> SChan Update -> ClientAcks -> IO ()
ackLoop ackChan updateChan clientAcks =  do
    clientAcks' <- sync $ ackEvtLoop clientAcks ackChan updateChan
    ackLoop ackChan updateChan clientAcks'

-- |Looping Event for multiple subsequent transactional synchronizations in the ackLoop
ackEvtLoop :: ClientAcks -> SChan AckUpdate -> SChan Update -> Evt ClientAcks
ackEvtLoop clientAcks ackChan updateChan = do
    update      <- recvEvt ackChan
    clientAcks' <- handleAck update clientAcks updateChan
    (alwaysEvt clientAcks') `chooseEvt` (ackEvtLoop clientAcks' ackChan updateChan)

-- |Handle an AckUpdate
handleAck :: AckUpdate -> ClientAcks -> SChan Update -> Evt ClientAcks
handleAck ackUpdate clientAcks updateChan = case ackUpdate of
    Response clientAckResponse  -> handleClientAckResponse clientAckResponse clientAcks updateChan
    Context ackContext          -> handleAckContext ackContext clientAcks updateChan
    ClientDisconnected clientId -> handleClientDisconnect clientAcks clientId updateChan

-- |Handle an ack response from a client. If there is a matching AckContext, handle the pair; if not,
-- add it to the ClientAcks.
handleClientAckResponse :: ClientAckResponse -> ClientAcks -> SChan Update -> Evt ClientAcks
handleClientAckResponse response@(ClientAckResponse clientId msgId frame) clientAcks updateChan = 
    case HM.lookup clientId clientAcks of
        Just (ackMap, responseMap) -> case HM.lookup msgId ackMap of
            Just context -> handleAckPair context (getResponseCommand response) clientAcks updateChan ackMap responseMap
            Nothing -> return $ HM.insert clientId (ackMap, HM.insert msgId  response responseMap) clientAcks
        Nothing -> return $ HM.insert clientId (HM.empty, HM.singleton msgId response) clientAcks

-- |Handle an AckContext update. If there is a matching ClientAckResponse, handle the pair; if not,
-- add it to the ClientAcks.
handleAckContext :: AckContext -> ClientAcks -> SChan Update -> Evt ClientAcks
handleAckContext context@(AckContext frame msgId clientId ackType sentClients) clientAcks updateChan =
    case HM.lookup clientId clientAcks of
        Just (ackMap, responseMap) -> case HM.lookup msgId responseMap of
            Just response -> handleAckPair context (getResponseCommand response) clientAcks updateChan ackMap responseMap
            Nothing -> return $ HM.insert clientId (HM.insert msgId context ackMap, responseMap) clientAcks
        Nothing -> return $ HM.insert clientId (HM.singleton msgId context, HM.empty) clientAcks

-- |Handle a client disconnect by resending its unack'd messages.
handleClientDisconnect :: ClientAcks -> ClientId -> SChan Update -> Evt ClientAcks
handleClientDisconnect clientAcks clientId updateChan = case HM.lookup clientId clientAcks of
    Just (contextMap, _) -> do
        mapM_ (resendContext updateChan) contextMap
        return $ HM.delete clientId clientAcks
    Nothing              -> return clientAcks

-- |Resend an AckContext
resendContext :: SChan Update -> AckContext -> Evt ThreadId
resendContext updateChan ackContext = forkEvt (alwaysEvt ()) (\_ -> resendContextFrame updateChan ackContext)

-- |Handle an AckContext with the Command received in response from the client (NACK or ACK)
handleAckPair :: AckContext -> Command -> ClientAcks -> SChan Update -> AckContextMap -> AckResponseMap -> Evt ClientAcks
handleAckPair context@(AckContext frame msgId clId ackType sentClients) cmd clientAcks updateChan ackMap responseMap = 
    let handleEvt = case cmd of
            ACK  -> alwaysEvt ()
            NACK -> case ackType of
                Client           -> do
                    -- Message IDs are ever increasing, and double as the ack ID, so we need to handle all messages with
                    -- keys less than or equal to the given message ID
                    forkEvt (alwaysEvt ()) (\_ -> handleNack (elems $ filterGtKeys msgId ackMap) updateChan)
                    alwaysEvt ()
                ClientIndividual -> do
                    -- Only handle a single message context
                    forkEvt (alwaysEvt ()) (\_ -> handleNack [context] updateChan)
                    alwaysEvt ()
        insertEvt = 
            case ackType of
                Client -> 
                    -- In Client mode, all messages sent before the message (n)ack'd are considered dealt with, so 
                    -- filter them out of the maps
                    return $ HM.insert clId (filterLeqKeys msgId ackMap, filterLeqKeys msgId responseMap) clientAcks
                ClientIndividual -> 
                    -- In ClientIndividual mode, only one message at a time is (n)ack'd
                    return $ HM.insert clId (HM.delete msgId ackMap, HM.delete msgId responseMap) clientAcks
    in do { handleEvt ; insertEvt }

-- |Convenience function; filter all keys less than or equal to the given key out of the HashMap containing ordinal keys
filterLeqKeys :: Ord a => a -> HashMap a b -> HashMap a b
filterLeqKeys n = HM.filterWithKey (\k -> \_ -> k > n)

-- |Convenience function; filter all keys greater than the given key out of the HashMap containing ordinal keys
filterGtKeys :: Ord a => a -> HashMap a b -> HashMap a b
filterGtKeys n = HM.filterWithKey (\k -> \_ -> k <= n)

-- |Resend all frames in the list of AckContexts
handleNack :: [AckContext] -> SChan Update -> IO ()
handleNack contexts updateChan = mapM_ (resendContextFrame updateChan) contexts

--- |Resend a Frame from an AckContext
resendContextFrame :: SChan Update -> AckContext -> IO ()
resendContextFrame updateChan (AckContext frame msgId _ _ sentClients) = do
    sync $ sendEvt updateChan $ ResendMessage (_getDestination frame) frame sentClients msgId
    return ()

-- |Resend a Frame whose send timed out (possibly due to no active subscribers)
resendTimedOutFrame :: Frame -> SChan Update -> Destination -> [ClientId] -> MessageId -> IO ()
resendTimedOutFrame frame updateChan dest sentClients messageId = do
    forkIO $ sync $ sendEvt updateChan $ ResendMessage dest frame sentClients messageId
    return ()

-- |State management loop for Subscriptions
updateLoop :: SChan Update -> SChan Update -> SChan AckUpdate -> Subscriptions -> Incrementer -> IO ()
updateLoop updateChan lowPriorityChan ackChan subs inc = do
        subs' <- sync $ updateEvtLoop updateChan lowPriorityChan ackChan subs inc
        updateLoop updateChan lowPriorityChan ackChan subs' inc

-- |Looping event to handle the possiblility multiple subsequent transactional synchronizations in the updateLoop
updateEvtLoop :: SChan Update -> SChan Update -> SChan AckUpdate -> Subscriptions -> Incrementer -> Evt Subscriptions
updateEvtLoop updateChan lowPriorityChan ackChan subs inc = do
    update <- (recvEvt updateChan) `chooseEvt` (timeOutEvt 100 `thenEvt` (\_ -> recvEvt lowPriorityChan))
    subs'  <- handleUpdate update subs updateChan lowPriorityChan ackChan inc
    (alwaysEvt subs') `chooseEvt` (updateEvtLoop updateChan lowPriorityChan ackChan subs' inc)

-- |Handle an Update received in the updateLoop
handleUpdate :: Update -> Subscriptions -> SChan Update -> SChan Update -> SChan AckUpdate -> Incrementer -> Evt Subscriptions
-- Add
handleUpdate (Add dest clientSub) subscriptions _ _ _ _ = do
    return $ addSubscription dest clientSub subscriptions
-- Remove
handleUpdate (Remove clientId subId) subs _ _ _ _ = do
    return $ removeSubscription clientId subId subs
-- GotMessage
handleUpdate (GotMessage dest frame) subs updateChan lowPriorityChan ackChan inc = do
    forkEvt (alwaysEvt ()) (\_ -> handleMessage frame dest subs [] Nothing ackChan lowPriorityChan inc)
    return subs
-- Resend message due to NACK response
handleUpdate (ResendMessage dest frame sentClients messageId) subs updateChan lowPriorityChan ackChan inc = do
    forkEvt (alwaysEvt ()) (\_ -> handleMessage frame dest subs sentClients (Just messageId) ackChan lowPriorityChan inc)
    return subs
-- Ack response from client
handleUpdate (Ack clientResponse) subs _ _ ackChan _ = do
    sendEvt ackChan (Response clientResponse)
    return subs
-- Client disconnected
handleUpdate (Disconnected clientId) subs@(Subscriptions subMap clientDests) _ _ ackChan _ = do
    sendEvt ackChan (ClientDisconnected clientId)
    return $ removeAllClientSubs subs clientId

-- |Remove all subscriptions for the given ClientId from the Subscriptions
removeAllClientSubs :: Subscriptions -> ClientId -> Subscriptions
removeAllClientSubs subs@(Subscriptions subMap clientDests) clientId = case HM.lookup clientId clientDests of
    Just destMap -> Subscriptions (deleteSubs subMap clientId (elems destMap)) (HM.delete clientId clientDests)
    Nothing      -> subs

-- |Delete all subscriptions from the SubMap
deleteSubs :: SubMap -> ClientId -> [Destination] -> SubMap
deleteSubs subMap _ []                 = subMap
deleteSubs subMap clientId (dest:rest) = case HM.lookup dest subMap of
    Just clientSubs -> deleteSubs (HM.insert dest (HM.delete clientId clientSubs) subMap) clientId rest
    Nothing         -> deleteSubs subMap clientId rest

-- |Add a new Subscription to the given Destination
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

-- |Remove the given SubscriptionId for the given ClientId
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

-- |Handle a SEND Frame for the given Destination
handleMessage :: Frame -> Destination -> Subscriptions -> [ClientId] -> Maybe MessageId -> SChan AckUpdate -> SChan Update -> Incrementer -> IO ()
handleMessage frame dest subs@(Subscriptions subMap _) sentClients maybeId ackChan updateChan inc =
    case HM.lookup dest subMap of
        Just clientSubs -> do
            messageId    <- getNewMessageId maybeId inc -- If this is not a resend, it gets a new messageId (TODO: re-evaluate; maybe we just want a new id for each message regardless)
            frame'       <- return $ addFrameHeaderFront (messageIdHeader (show messageId)) frame
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
                -- If no Subscription synchronized on the send due to a timeout, trigger a resend
                Nothing -> do { forkIO $ resendTimedOutFrame frame updateChan dest sentClients messageId ; return () }
        Nothing -> return ()

getNewMessageId :: Maybe MessageId -> Incrementer -> IO MessageId
getNewMessageId maybeId inc = case maybeId of
    Just messageId -> return messageId
    Nothing        -> getNext inc

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
            _    -> addFrameHeaderFront (ackHeader (show messageId)) frame