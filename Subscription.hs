-- |The Subscription module deals with managing subscriptions on the STOMP broker.
module Subscription (
    initManager,
    addSubscription,
    removeSubscription,
    reportMessage,
    ClientId,
    SubscriptionManager,
    Subscription
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Control.Exception
import Data.HashMap.Strict as HM
import Data.Unique
import Stomp.Frames
import Stomp.Frames.IO

-- |A Dest is a unique Destination descriptor
type Dest                  = String

-- |A SubscriptionId is a String that is given by the client to identify a client subscription.
type SubscriptionId        = String

-- |A ClientId is used by the server to uniquely identify client subscription lists.
type ClientId              = Int

-- |A Subscription encapsulates information about a client subscription.
data Subscription          = Subscription ClientId FrameHandler SubscriptionId Dest

-- |A Topic maps client IDs to lists of subscriptions. A single client may have multiple subscriptions
-- to the same topic.
data Topic                 = Topic (HashMap ClientId [Subscription])

-- |A Destinations object maps a Dest (unique String destination descriptor) to a Topic.
data Destinations          = Destinations (HashMap Dest Topic)

-- |Updates are the types of events that can be received by a SubscriptionManger.
data Update                = Add Subscription |
                             Remove Subscription |
                             GotMessage Dest Frame |
                             Success |
                             Error SubscriptionException

-- |A SubscriptionManager handles updates to subscriptions.
data SubscriptionManager   = SubscriptionManager (SChan Update)

data SubscriptionException = ClientNotSubscribed Dest |
                             DestinationDoesNotExist Dest |
                             NoDestinationHeader

instance Show Subscription where
    show (Subscription clientId _ subId dest) = 
        "[" ++ (show clientId) ++ (", ") ++ (show subId) ++ ", " ++ dest ++ "]"

instance Show Topic where 
    show (Topic tMap) = "{Topic " ++ (show tMap) ++ "}"

instance Show Destinations where
    show (Destinations dMap) = "{Destinations " ++ show dMap ++ "}"

instance Show Update where
    show (Add sub)        = "(Add, " ++ (show sub) ++ ")"
    show (Remove sub)     = "(Remove, " ++ (show sub) ++ ")"
    show (GotMessage d f) = "(GotMessage, " ++ ", " ++ d ++ (show f) ++ ")"
    show Success          = "Success"
    show (Error e)        = "Error: " ++ (show e)

instance Exception SubscriptionException
instance Show SubscriptionException where
    show (ClientNotSubscribed dest)     = "Client is not subscribed to " ++ dest
    show (DestinationDoesNotExist dest) = "Destination " ++ dest ++ " does not exist"
    show NoDestinationHeader            = "Received a frame without a destination header"

-- |The initManager function initializes a SubscriptionManager and returns it to the caller.
initManager :: IO SubscriptionManager
initManager = do
    updateChan   <- sync newSChan
    destinations <- return $ Destinations empty
    forkIO $ notificationLoop updateChan destinations
    return $ SubscriptionManager updateChan

-- |Add a new subscription using the SubscriptionManager
addSubscription :: SubscriptionManager -> ClientId -> FrameHandler -> SubscriptionId -> Dest -> IO Subscription
addSubscription manager clientId handler subId dest = 
    let subscription = (Subscription clientId handler subId dest) in do
        handleEventNotification (Add subscription) manager
        return subscription

-- |Remove a subscription using the SubscriptionManager
removeSubscription :: SubscriptionManager -> Subscription -> IO ()
removeSubscription manager subscription = do
    handleEventNotification (Remove subscription) manager

-- |Send a SEND Frame to any interested subscribers.
reportMessage :: SubscriptionManager -> Frame -> IO ()
reportMessage manager frame = 
    case (getDestination frame) of
        Just dest -> handleEventNotification (GotMessage dest frame) manager
        Nothing   -> throw NoDestinationHeader

-- |Helper method to send an event notification on the SubscriptionManager's update channel
handleEventNotification :: Update -> SubscriptionManager -> IO ()
handleEventNotification event (SubscriptionManager chan) = do
    result <- sync $ (sendEvt chan event) `thenEvt` (\_ -> recvEvt chan)
    case result of 
        Success -> return ()
        Error e -> throw e
        _       -> error $ "Got an unexpected result: " ++ (show result)

-- |This function loops, blocking until a new Update is received on the channel. Depending on the
-- nature of the Update, the value in Destinations may change (e.g. as subscriptions are added or
-- removed).
notificationLoop :: SChan Update -> Destinations -> IO ()
notificationLoop updateChan destinations = do
    (destinations', update) <- sync $ ((recvEvt updateChan) `thenEvt` (handleUpdate destinations updateChan))
    handleNotifications update destinations'
    notificationLoop updateChan destinations'

-- |Handle necessary notifications (if any) given the Update.
handleNotifications :: Update -> Destinations -> IO ()
handleNotifications (GotMessage dest frame) (Destinations topicMap) = 
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> sendMessageToSubs frame clientSubs
        Nothing                 -> return ()
handleNotifications _ _ = return ()

-- |Construct an Evt to handle a given Update. Synchronizing on this Event returns a tuple containing the
-- updated Destinations and the Update with which the Evt was constructed to the caller.
handleUpdate :: Destinations -> SChan Update -> Update -> Evt (Destinations, Update)

-- Handle an Add Subscription Update
handleUpdate d@(Destinations topicMap) updateChan u@(Add sub@(Subscription _ _ _ dest)) = 
    (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt $ ((Destinations $ insert dest (getTopic d sub) topicMap, u)))

-- Handle a Remove Subscription Update
handleUpdate d@(Destinations topicMap) updateChan u@(Remove sub@(Subscription _ _ _ dest)) = 
    case removeSub d sub of
        Just topic ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt $ ((Destinations $ insert dest topic topicMap), u))
        Nothing    ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt (d, u))

-- Handle a GotMessage Update
handleUpdate d@(Destinations topicMap) updateChan u@(GotMessage dest frame) = do
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt (d, u))
        Nothing                 ->
            (sendEvt updateChan (Error $ DestinationDoesNotExist dest)) `thenEvt` (\_ -> alwaysEvt (d, u))

-- |Given a Destinations and a Subscription, return the updated Topic were that Subscription to be added. If the
-- Topic does not exist in the Destinations, a new Topic with only the Subscription will be returned.
getTopic :: Destinations -> Subscription -> Topic
getTopic d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
            Just (Topic clientSubs) -> case HM.lookup clientId clientSubs of
                Just subList -> Topic (HM.insert clientId (sub:subList) clientSubs)
                Nothing      -> Topic (HM.insert clientId [sub] clientSubs)
            Nothing                 -> Topic (singleton clientId [sub])

-- |Return the Topic that is the result of removing the Subscription.
removeSub :: Destinations -> Subscription -> (Maybe Topic)
removeSub d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> Just $ Topic (HM.delete clientId clientSubs)
        Nothing                 -> Nothing

-- |Send a MESSAGE Frame constructed from the given SEND Frame to all clients in the Subscription map. 
sendMessageToSubs :: Frame -> (HashMap ClientId [Subscription]) -> IO ()
sendMessageToSubs frame subMap = do
    unique <- newUnique
    frame  <- return $ addFrameHeaderFront (messageIdHeader $ show $ hashUnique unique) frame
    mapM_ (sendMessage frame) subMap

-- |Send the Frame to all Subscriptions in the list.
sendMessage :: Frame -> [Subscription] -> IO ()
sendMessage _ [] = return ()
sendMessage frame (sub@(Subscription _ handler subId _):rest) = do
    forkIO $ put handler (transformMessage frame sub)
    sendMessage frame rest

-- |Transform the Frame into a MESSAGE Frame.
transformMessage :: Frame -> Subscription -> Frame
transformMessage (Frame _ headers body) (Subscription _ _ subId _) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body
