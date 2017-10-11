module Subscription where

import Control.Concurrent
import Control.Concurrent.TxEvent
import Control.Exception
import Data.HashMap.Strict as HM
import Data.Unique
import Stomp.Frames
import Stomp.Frames.IO

type Dest= String
type ClientId = Integer
type SubscriptionId = String

data Client = Client ClientId FrameHandler SubscriptionId

data Topic = Topic (HashMap Integer Client)

data Destinations = Destinations (HashMap Dest Topic)

data Update = Add Client Dest |
              Remove Client Dest |
              GotMessage Dest Frame

data SubscriptionNotifier = SubscriptionNotifier (SChan Update)

data SubscriptionException = ClientNotSubscribed Dest |
                             DestinationDoesNotExist Dest

instance Exception SubscriptionException
instance Show SubscriptionException where
    show (ClientNotSubscribed dest)     = "Client is not subscribed to " ++ dest
    show (DestinationDoesNotExist dest) = "Destination " ++ dest ++ " does not exist"

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
handleUpdate (Add client dest) d@(Destinations topicMap) = 
    return $ Destinations $ insert dest (addClient d client dest) topicMap
handleUpdate (Remove client dest) d@(Destinations topicMap) = 
    return $ Destinations $ insert dest (removeClient d client dest) topicMap
handleUpdate (GotMessage dest frame) d@(Destinations topicMap) = do
    case HM.lookup dest topicMap of
        Just (Topic clientMap) -> sendMessageToClients frame clientMap
        Nothing                -> throw $ DestinationDoesNotExist dest
    return d

addClient :: Destinations -> Client -> Dest -> Topic
addClient d@(Destinations topicMap) c@(Client cid _ _) dest =
    case HM.lookup dest topicMap of
            Just (Topic clientMap) -> Topic (HM.insert cid c clientMap)
            Nothing                -> Topic (singleton cid c)

removeClient :: Destinations -> Client -> Dest -> Topic
removeClient d@(Destinations topicMap) c@(Client cid _ _) dest =
    case HM.lookup dest topicMap of
        Just (Topic clientMap) -> Topic (HM.delete cid clientMap)
        Nothing                -> throw $ ClientNotSubscribed dest

sendMessageToClients :: Frame -> (HashMap Integer Client) -> IO ()
sendMessageToClients frame clientMap = mapM_ (sendMessage frame) clientMap

sendMessage :: Frame -> Client -> IO ()
sendMessage frame c@(Client _ handler subscriptionId) = do
    unique <- newUnique
    frame' <- return $ addFrameHeaderFront (messageIdHeader $ show $ hashUnique unique) frame
    forkIO $ put handler (transformMessage frame' c)
    return ()

transformMessage :: Frame -> Client -> Frame
transformMessage (Frame _ headers body) (Client _ _ subscriptionId) = 
    Frame MESSAGE (addHeaderFront (subscriptionHeader subscriptionId) headers) body
