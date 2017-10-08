module Subscription where

import Control.Concurrent
import Data.HashMap.Strict as HM
import Stomp.Frames
import Stomp.Frames.IO

data AckType = Auto | Client | ClientIndividual

data Subscriber = Subscriber FrameHandler AckType

data Topic = Topic String [Subscriber]

type Subscriptions = HashMap String Topic


newTopic :: String -> Topic
newTopic s = Topic s []

sendToAll :: Frame -> Topic -> IO ()
sendToAll _ (Topic _ []) = return ()
sendToAll frame (Topic name (sub:subs)) = do
    sendToSubscriber frame sub
    sendToAll frame (Topic name subs)

sendToSubscriber :: Frame -> Subscriber -> IO ThreadId
sendToSubscriber frame (Subscriber handler _) = do
    forkIO $ put handler frame

addTopicSubscriber :: Subscriber -> Topic -> Topic
addTopicSubscriber subscriber (Topic name subs) = Topic name (subscriber:subs)

addSubscriber :: Subscriber -> String -> Subscriptions -> Maybe Subscriptions
addSubscriber subscriber topicName subs = case HM.lookup topicName subs of
    Just topic -> Just $ HM.insert topicName (addTopicSubscriber subscriber topic) subs
    Nothing    -> Nothing

initSubscriptions :: Subscriptions
initSubscriptions = HM.empty

addTopic :: String -> Subscriptions -> Subscriptions
addTopic name subs = HM.insert name (newTopic name) subs

sendToTopic :: Subscriptions -> Frame -> String -> IO ()
sendToTopic subs frame topicName = case HM.lookup topicName subs of
    Just topic -> sendToAll frame topic
    Nothing    -> return ()

getTopic :: String -> Subscriptions -> Maybe Topic
getTopic topicName subs = HM.lookup topicName subs
