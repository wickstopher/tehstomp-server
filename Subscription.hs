module Subscription (
    initNotifier,
    addSubscription,
    removeSubscription,
    reportMessage,
    ClientId,
    Notifier,
    Subscription
) where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Control.Exception
import Data.HashMap.Strict as HM
import Data.Unique
import Stomp.Frames
import Stomp.Frames.IO

type Dest                  = String
type SubscriptionId        = String
type ClientId              = Int

data Subscription          = Subscription ClientId FrameHandler SubscriptionId Dest

data Topic                 = Topic (HashMap ClientId [Subscription])

data Destinations          = Destinations (HashMap Dest Topic)

data Update                = Add Subscription |
                             Remove Subscription |
                             GotMessage Dest Frame |
                             Success |
                             Error SubscriptionException

data Notifier              = Notifier (SChan Update)

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

initNotifier :: IO Notifier
initNotifier = do
    updateChan   <- sync newSChan
    destinations <- return $ Destinations empty
    forkIO $ notificationLoop updateChan destinations
    return $ Notifier updateChan

notificationLoop :: SChan Update -> Destinations -> IO ()
notificationLoop updateChan destinations = do
    (destinations', update) <- sync $ ((recvEvt updateChan) `thenEvt` (handleUpdate destinations updateChan))
    handleNotifications update destinations'
    notificationLoop updateChan destinations'

handleNotifications :: Update -> Destinations -> IO ()
handleNotifications (GotMessage dest frame) (Destinations topicMap) = 
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> sendMessageToSubs frame clientSubs
        Nothing                 -> return ()
handleNotifications _ _ = return ()

handleUpdate :: Destinations -> SChan Update -> Update -> Evt (Destinations, Update)
handleUpdate d@(Destinations topicMap) updateChan u@(Add sub@(Subscription _ _ _ dest)) = 
    (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt $ ((Destinations $ insert dest (addSub d sub) topicMap, u)))

handleUpdate d@(Destinations topicMap) updateChan u@(Remove sub@(Subscription _ _ _ dest)) = 
    case removeSub d sub of
        Just topic ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt $ ((Destinations $ insert dest topic topicMap), u))
        Nothing    ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt (d, u))

handleUpdate d@(Destinations topicMap) updateChan u@(GotMessage dest frame) = do
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) ->
            (sendEvt updateChan Success) `thenEvt` (\_ -> alwaysEvt (d, u))
        Nothing                 ->
            (sendEvt updateChan (Error $ DestinationDoesNotExist dest)) `thenEvt` (\_ -> alwaysEvt (d, u))

addSub :: Destinations -> Subscription -> Topic
addSub d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
            Just (Topic clientSubs) -> case HM.lookup clientId clientSubs of
                Just subList -> Topic (HM.insert clientId (sub:subList) clientSubs)
                Nothing      -> Topic (HM.insert clientId [sub] clientSubs)
            Nothing                 -> Topic (singleton clientId [sub])

removeSub :: Destinations -> Subscription -> (Maybe Topic)
removeSub d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> Just $ Topic (HM.delete clientId clientSubs)
        Nothing                 -> Nothing

sendMessageToSubs :: Frame -> (HashMap ClientId [Subscription]) -> IO ()
sendMessageToSubs frame subMap = do
    unique <- newUnique
    frame  <- return $ addFrameHeaderFront (messageIdHeader $ show $ hashUnique unique) frame
    mapM_ (sendMessage frame) subMap

sendMessage :: Frame -> [Subscription] -> IO ()
sendMessage _ [] = return ()
sendMessage frame (sub@(Subscription _ handler subId _):rest) = do
    forkIO $ put handler (transformMessage frame sub)
    sendMessage frame rest

transformMessage :: Frame -> Subscription -> Frame
transformMessage (Frame _ headers body) (Subscription _ _ subId _) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body

addSubscription :: Notifier -> ClientId -> FrameHandler -> SubscriptionId -> Dest -> IO Subscription
addSubscription notifier clientId handler subId dest = 
    let subscription = (Subscription clientId handler subId dest) in do
        handleEventNotification (Add subscription) notifier
        return subscription

removeSubscription :: Notifier -> Subscription -> IO ()
removeSubscription notifier client = do
    handleEventNotification (Remove client) notifier

reportMessage :: Notifier -> Frame -> IO ()
reportMessage notifier frame = 
    case (getDestination frame) of
        Just dest -> handleEventNotification (GotMessage dest frame) notifier
        Nothing   -> throw NoDestinationHeader

handleEventNotification :: Update -> Notifier -> IO ()
handleEventNotification event (Notifier chan) = do
    result <- sync $ (sendEvt chan event) `thenEvt` (\_ -> recvEvt chan)
    case result of 
        Success -> return ()
        Error e -> throw e
        _       -> error $ "Got an unexpected result: " ++ (show result)
