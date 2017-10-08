import Control.Concurrent
import Data.ByteString as BS
import Data.List as List
import Stomp.Frames.IO
import Network.Socket hiding (close)
import Prelude hiding (log)
import System.IO as IO
import System.Environment
import Stomp.Frames
import Stomp.TLogger
import Subscription

main :: IO ()
main = do
    args <- getArgs
    port <- processArgs args
    console <- dateTimeLogger stdout
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet port iNADDR_ANY)
    listen sock 5
    log console $ "STOMP broker initiated on port " ++ (show port)
    socketLoop sock console initSubscriptions

processArgs :: [String] -> IO PortNumber
processArgs (s:[]) = return $ fromIntegral ((read s)::Int)
processArgs _      = return 2323

socketLoop :: Socket -> Logger -> Subscriptions -> IO ()
socketLoop sock console subs = do
    (uSock, _) <- accept sock
    addr <- getPeerName uSock
    log console $ "New connection received from " ++ (show addr)
    handle <- socketToHandle uSock ReadWriteMode
    hSetBuffering handle NoBuffering
    frameHandler <- initFrameHandler handle
    forkIO $ negotiateConnection frameHandler (addTransform (stringTransform ("[" ++ show addr ++ "]")) console) subs
    socketLoop sock console subs

negotiateConnection :: FrameHandler -> Logger -> Subscriptions -> IO ()
negotiateConnection frameHandler console subs = do
    frame <- get frameHandler
    case (getCommand frame) of
        STOMP   -> do
            log console "STOMP frame received; negotiating new connection"
            handleNewConnection frameHandler frame console subs
        CONNECT -> do
            log console "CONNECT frame received; negotiating new connection"
            handleNewConnection frameHandler frame console subs
        _       -> do
            log console $ (show $ getCommand frame) ++ " frame received; rejecting connection"
            rejectConnection frameHandler "Please initiate communications with a connection request"
    
handleNewConnection :: FrameHandler -> Frame -> Logger -> Subscriptions -> IO ()
handleNewConnection frameHandler frame console subs = let version = determineVersion frame in
    case version of
        Just v  -> do 
            sendConnectedResponse frameHandler v
            log console $ "Connection initiated to client using STOMP protocol version " ++ v
            connectionLoop frameHandler console subs
        Nothing -> do
            log console "No common protocol versions supported; rejecting connection"
            rejectConnection frameHandler ("Supported STOMP versions are: " ++  supportedVersionsAsString)

connectionLoop :: FrameHandler -> Logger -> Subscriptions -> IO ()
connectionLoop frameHandler console subs = do
    frame <- get frameHandler
    log console $ "Received " ++ (show $ getCommand frame) ++ " frame"
    handleReceiptRequest frameHandler frame console
    case (getCommand frame) of
        DISCONNECT -> do 
            log console "Disconnect request received; closing connection to client"
            close frameHandler
        SEND       -> handleSendFrame frame console
        SUBSCRIBE  -> do 
            updatedSubs <- handleSubscriptionRequest frameHandler frame console subs
            case updatedSubs of
                Just subs' -> connectionLoop frameHandler console subs'
                Nothing    -> rejectConnection frameHandler "There was a problem adding your subscription request"
        _          -> log console "Handler not yet implemented"
    connectionLoop frameHandler console subs

handleSendFrame :: Frame -> Logger -> IO ()
handleSendFrame frame console = case getDestination frame of
    Just destination -> do
        log console $ "Message destination: " ++ destination
        log console $ "Message contents: " ++ (show $ getBody frame)
    Nothing -> log console "No destination specified in SEND frame"

handleSubscriptionRequest :: FrameHandler -> Frame -> Logger -> Subscriptions -> IO (Maybe Subscriptions)
handleSubscriptionRequest handler frame console subs = 
    case getDestination frame of
        Just destination -> let subscriber = Subscriber handler (getAckType $ getAck frame) in do
                return $ addSubscriber subscriber destination subs
        Nothing -> do
            rejectConnection handler "No destination header present in subscription request."
            return $ Just subs

getAckType :: Maybe String -> AckType
getAckType (Just "client")             = Client
getAckType (Just "client-individual)") = ClientIndividual
getAckType _                           = Auto 

updateSubs :: String -> Subscriptions -> IO Subscriptions
updateSubs destination subs = case getTopic destination subs of
    Just topic -> return subs
    Nothing    -> return $ addTopic destination subs

sendConnectedResponse :: FrameHandler -> String -> IO ()
sendConnectedResponse frameHandler version = let response = connected version in
    do put frameHandler response

rejectConnection :: FrameHandler -> String -> IO ()
rejectConnection frameHandler message = let response = errorFrame message in
    do  put frameHandler response
        close frameHandler

determineVersion :: Frame -> Maybe String
determineVersion frame = 
    let clientVersions = getSupportedVersions frame in
        case clientVersions of 
            Just versions -> getHighestSupportedVersion versions
            Nothing       -> Nothing

getHighestSupportedVersion :: [String] -> Maybe String
getHighestSupportedVersion clientVersions = 
    let mutualVersions = intersect clientVersions supportedVersions in
        maybeMax mutualVersions

maybeMax :: Ord a => [a] -> Maybe a
maybeMax [] = Nothing
maybeMax xs = Just (List.maximum xs)

supportedVersions :: [String]
supportedVersions = ["1.2"]

supportedVersionsAsString :: String
supportedVersionsAsString = List.intercalate ", " supportedVersions

handleReceiptRequest :: FrameHandler -> Frame -> Logger -> IO ()
handleReceiptRequest frameHandler frame console = do
    case (getReceipt frame) of
        Just receiptId -> do
            log console $ "Sending receipt for message with receipt ID: " ++ receiptId
            sendReceipt frameHandler receiptId
        _ -> return ()

sendReceipt :: FrameHandler -> String -> IO ()
sendReceipt frameHandler receiptId = do
    put frameHandler $ receipt receiptId

stringTransform :: String -> IO String -> IO String
stringTransform string ioString = do
    s' <- ioString
    return $ string ++ " " ++ s'
