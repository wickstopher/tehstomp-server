module Subscription where

import Control.Concurrent
import Data.HashMap.Strict as HM
import Stomp.Frames
import Stomp.Frames.IO

data Subscriber = Subscriber FrameHandler

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
sendToSubscriber frame (Subscriber handler) = do
    forkIO $ put handler frame

addSubscriber :: FrameHandler -> Topic -> Topic
addSubscriber handler (Topic name subs) = Topic name ((Subscriber handler):subs)

initSubscriptions :: Subscriptions
initSubscriptions = HM.empty

addTopic :: String -> Subscriptions -> Subscriptions
addTopic name subs = HM.insert name (newTopic name) subs

sendToTopic :: Subscriptions -> Frame -> String -> IO ()
sendToTopic subs frame topicName = case HM.lookup topicName subs of
    Just topic -> sendToAll frame topic
    Nothing    -> return ()

