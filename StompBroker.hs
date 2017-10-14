import Control.Concurrent
import Control.Exception
import Data.ByteString as BS
import Data.List as List
import Data.Unique
import Stomp.Frames.IO
import Network.Socket hiding (close)
import Prelude hiding (log)
import System.IO as IO
import System.Environment
import Stomp.Frames
import Stomp.TLogger
import Subscription

data ClientException = NoIdHeader |
                       NoDestinationHeader  |
                       SubscriptionUpdate

instance Exception ClientException
instance Show ClientException where
    show NoIdHeader          = "No id header present in request"
    show NoDestinationHeader = "No destination header present in request"
    show SubscriptionUpdate  = "There was an error processing the subscription request"

main :: IO ()
main = do
    args     <- getArgs
    port     <- processArgs args
    console  <- dateTimeLogger stdout
    notifier <- initNotifier
    sock     <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet port iNADDR_ANY)
    listen sock 5
    log console $ "STOMP broker initiated on port " ++ (show port)
    socketLoop sock console notifier

processArgs :: [String] -> IO PortNumber
processArgs (s:[]) = return $ fromIntegral ((read s)::Int)
processArgs _      = return 2323

socketLoop :: Socket -> Logger -> Notifier -> IO ()
socketLoop sock console notifier = do
    (uSock, _) <- accept sock
    addr <- getPeerName uSock
    log console $ "New connection received from " ++ (show addr)
    handle <- socketToHandle uSock ReadWriteMode
    hSetBuffering handle NoBuffering
    frameHandler <- initFrameHandler handle
    forkIO $ negotiateConnection frameHandler (addTransform (stringTransform ("[" ++ show addr ++ "]")) console) notifier
    socketLoop sock console notifier

negotiateConnection :: FrameHandler -> Logger -> Notifier -> IO ()
negotiateConnection frameHandler console notifier = do
    frame <- get frameHandler
    case (getCommand frame) of
        STOMP   -> do
            log console "STOMP frame received; negotiating new connection"
            handleNewConnection frameHandler frame console notifier
        CONNECT -> do
            log console "CONNECT frame received; negotiating new connection"
            handleNewConnection frameHandler frame console notifier
        _       -> do
            log console $ (show $ getCommand frame) ++ " frame received; rejecting connection"
            rejectConnection console frameHandler "Please initiate communications with a connection request"
    
handleNewConnection :: FrameHandler -> Frame -> Logger -> Notifier -> IO ()
handleNewConnection frameHandler frame console notifier = let version = determineVersion frame in
    case version of
        Just v  -> do 
            sendConnectedResponse frameHandler v
            uniqueId <- newUnique
            log console $ "Connection initiated to client using STOMP protocol version " ++ v
            log console $ "Client unique ID is " ++ (show $ hashUnique uniqueId)
            connectionLoop frameHandler console notifier (hashUnique uniqueId) []
        Nothing -> do
            log console "No common protocol versions supported; rejecting connection"
            rejectConnection console frameHandler ("Supported STOMP versions are: " ++  supportedVersionsAsString)

connectionLoop :: FrameHandler -> Logger -> Notifier -> ClientId -> [Subscription] -> IO ()
connectionLoop frameHandler console notifier clientId subs = do
    result <- try (handleNextFrame frameHandler console notifier clientId subs) 
        :: IO (Either SomeException (Command, [Subscription]))
    case result of
        Left exception -> do
            log console $ "There was an error processing a client frame: " ++ (show exception)
            rejectConnection console frameHandler ("Error: " ++ (show exception))
        Right (command, subs)-> case command of
            DISCONNECT -> return ()
            _          -> connectionLoop frameHandler console notifier clientId subs

handleNextFrame :: FrameHandler -> Logger -> Notifier -> ClientId -> [Subscription] -> IO (Command, [Subscription])
handleNextFrame frameHandler console notifier clientId subs = do 
    frame <- get frameHandler
    command <- return $ getCommand frame
    log console $ "Received " ++ (show command) ++ " frame"
    handleReceiptRequest frameHandler frame console
    case command of
        DISCONNECT -> do 
            log console "Disconnect request received; closing connection to client"
            close frameHandler
            return (command, subs)
        SEND       -> do 
            handleSendFrame frame console notifier
            return (command, subs)
        SUBSCRIBE  -> do
            newSub <- handleSubscriptionRequest frameHandler frame notifier clientId
            return (command, newSub:subs)
        _          -> do
            log console "Handler not yet implemented"
            return (command, subs)

handleSendFrame :: Frame -> Logger -> Notifier -> IO ()
handleSendFrame frame console notifier = reportMessage notifier frame

handleSubscriptionRequest :: FrameHandler -> Frame -> Notifier -> ClientId -> IO Subscription
handleSubscriptionRequest handler frame notifier clientId = 
    let (maybeDest, maybeId) = (getDestination frame, getId frame) in
        getNewSub maybeDest maybeId handler notifier clientId

getNewSub :: Maybe String -> Maybe String -> FrameHandler -> Notifier -> ClientId -> IO Subscription
getNewSub Nothing _ _ _ _ = throw NoDestinationHeader
getNewSub _ Nothing _ _ _ = throw NoIdHeader
getNewSub (Just dest) (Just subId) handler notifier clientId = 
    addSubscription notifier clientId handler subId dest

-- getAckType :: Maybe String -> AckType
-- getAckType (Just "client")             = Client
-- getAckType (Just "client-individual)") = ClientIndividual
-- getAckType _                           = Auto



sendConnectedResponse :: FrameHandler -> String -> IO ()
sendConnectedResponse frameHandler version = let response = connected version in
    do put frameHandler response

rejectConnection :: Logger -> FrameHandler -> String -> IO ()
rejectConnection console frameHandler message = let response = errorFrame message in do  
    log console "Rejecting connection!"
    put frameHandler response
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
