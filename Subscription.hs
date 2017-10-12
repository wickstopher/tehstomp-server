module Subscription (
    initNotifier,
    addSubscription,
    removeSubscription,
    reportMessage
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
type ClientId              = Integer

data Subscription          = Subscription ClientId FrameHandler SubscriptionId Dest

data Topic                 = Topic (HashMap ClientId Subscription)

data Destinations          = Destinations (HashMap Dest Topic)

data Update                = Add Subscription |
                             Remove Subscription |
                             GotMessage Dest Frame

data SubscriptionNotifier  = SubscriptionNotifier (SChan Update)

data SubscriptionException = ClientNotSubscribed Dest |
                             DestinationDoesNotExist Dest |
                             NoDestinationHeader

instance Exception SubscriptionException
instance Show SubscriptionException where
    show (ClientNotSubscribed dest)     = "Client is not subscribed to " ++ dest
    show (DestinationDoesNotExist dest) = "Destination " ++ dest ++ " does not exist"
    show NoDestinationHeader            = "Received a frame without a destination header"

initNotifier :: IO SubscriptionNotifier
initNotifier = do
    updateChan   <- sync newSChan
    destinations <- return $ Destinations empty
    forkIO $ notificationLoop updateChan destinations
    return $ SubscriptionNotifier updateChan

notificationLoop :: SChan Update -> Destinations -> IO ()
notificationLoop updateChan destinations = do
    update <- sync $ recvEvt updateChan
    destinations' <- handleUpdate update destinations
    notificationLoop updateChan destinations'

handleUpdate :: Update -> Destinations -> IO Destinations
handleUpdate (Add sub@(Subscription _ _ _ dest)) d@(Destinations topicMap) = 
    return $ Destinations $ insert dest (addSub d sub) topicMap
handleUpdate (Remove sub@(Subscription _ _ _ dest)) d@(Destinations topicMap) = 
    return $ Destinations $ insert dest (removeSub d sub) topicMap
handleUpdate (GotMessage dest frame) d@(Destinations topicMap) = do
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> sendMessageToSubs frame clientSubs
        Nothing             -> throw $ DestinationDoesNotExist dest
    return d

addSub :: Destinations -> Subscription -> Topic
addSub d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
            Just (Topic clientSubs) -> Topic (HM.insert clientId sub clientSubs)
            Nothing                 -> Topic (singleton clientId sub)

removeSub :: Destinations -> Subscription -> Topic
removeSub d@(Destinations topicMap) sub@(Subscription clientId _ _ dest) =
    case HM.lookup dest topicMap of
        Just (Topic clientSubs) -> Topic (HM.delete clientId clientSubs)
        Nothing                 -> throw $ ClientNotSubscribed dest

sendMessageToSubs :: Frame -> (HashMap Integer Subscription) -> IO ()
sendMessageToSubs frame subMap = mapM_ (sendMessage frame) subMap

sendMessage :: Frame -> Subscription -> IO ()
sendMessage frame sub@(Subscription _ handler subId _) = do
    unique <- newUnique
    frame' <- return $ addFrameHeaderFront (messageIdHeader $ show $ hashUnique unique) frame
    forkIO $ put handler (transformMessage frame' sub)
    return ()

transformMessage :: Frame -> Subscription -> Frame
transformMessage (Frame _ headers body) (Subscription _ _ subId _) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subId) headers) body

addSubscription :: SubscriptionNotifier -> ClientId -> FrameHandler -> SubscriptionId -> Dest -> IO Subscription
addSubscription (SubscriptionNotifier chan) clientId handler subId dest = 
    let subscription = (Subscription clientId handler subId dest) in do
        sync $ sendEvt chan (Add subscription)
        return subscription

removeSubscription :: SubscriptionNotifier -> Subscription -> IO ()
removeSubscription (SubscriptionNotifier chan) client = sync $ sendEvt chan (Remove client)

reportMessage :: SubscriptionNotifier -> Frame -> IO ()
reportMessage (SubscriptionNotifier chan) frame = 
    case (getDestination frame) of
        Just dest -> sync $ sendEvt chan (GotMessage dest frame)
        Nothing   -> throw NoDestinationHeader
