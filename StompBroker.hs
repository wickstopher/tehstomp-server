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

-- |Set up the environment and initialize the socket loop.
main :: IO ()
main = do
    args       <- getArgs
    port       <- processArgs args
    console    <- dateTimeLogger stdout
    subManager <- initManager
    sock       <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet port iNADDR_ANY)
    listen sock 5
    log console $ "STOMP broker initiated on port " ++ (show port)
    socketLoop sock console subManager

-- |Process the command-line arguments.
processArgs :: [String] -> IO PortNumber
processArgs (s:[]) = return $ fromIntegral ((read s)::Int)
processArgs _      = return 2323

-- |Loop as connections are received, forking off a new thread for each connection.
socketLoop :: Socket -> Logger -> SubscriptionManager -> IO ()
socketLoop sock console subManager = do
    (uSock, _) <- accept sock
    addr <- getPeerName uSock
    log console $ "New connection received from " ++ (show addr)
    handle <- socketToHandle uSock ReadWriteMode
    hSetBuffering handle NoBuffering
    frameHandler <- initFrameHandler handle
    forkIO $ negotiateConnection frameHandler (addTransform (appendTransform ("[" ++ show addr ++ "]")) console) subManager
    socketLoop sock console subManager

-- |Negotiate client connection. 
negotiateConnection :: FrameHandler -> Logger -> SubscriptionManager -> IO ()
negotiateConnection frameHandler console subManager = do
    f <- get frameHandler
    case f of 
        NewFrame frame -> case (getCommand frame) of
            STOMP   -> do
                log console "STOMP frame received; negotiating new connection"
                handleNewConnection frameHandler frame console subManager
            CONNECT -> do
                log console "CONNECT frame received; negotiating new connection"
                handleNewConnection frameHandler frame console subManager
            _       -> do
                log console $ (show $ getCommand frame) ++ " frame received; rejecting connection"
                rejectConnection console frameHandler "Please initiate communications with a connection request"
        GotEof -> do
            log console "Client disconnected before connection could be negotiated."
        ParseError msg -> do
            log console $ "There was an error parsing the frame from the client: " ++ msg

-- |Handle a new client connection following the receipt of a CONNECT Frame. This ensures that there is a shared
-- protocol version, and if there is, sends a CONNECTED Frame. and creates a new Unique (with respect to this 
-- server instance) client ID, and initializes the connection loop that listens for Frames on the client's handle.
handleNewConnection :: FrameHandler -> Frame -> Logger -> SubscriptionManager -> IO ()
handleNewConnection frameHandler frame console subManager = let version = determineVersion frame in
    case version of
        Just v  -> do 
            sendConnectedResponse frameHandler v
            uniqueId <- newUnique
            log console $ "Connection initiated to client using STOMP protocol version " ++ v
            log console $ "Client unique ID is " ++ (show $ hashUnique uniqueId)
            connectionLoop frameHandler console subManager (hashUnique uniqueId) []
        Nothing -> do
            log console "No common protocol versions supported; rejecting connection"
            rejectConnection console frameHandler ("Supported STOMP versions are: " ++  supportedVersionsAsString)

-- |Helper function for handleNewConnection; sends the CONNECTED Frame
sendConnectedResponse :: FrameHandler -> String -> IO ()
sendConnectedResponse frameHandler version = let response = connected version in
    do put frameHandler response

-- |Reject the Connection and close the handle.
rejectConnection :: Logger -> FrameHandler -> String -> IO ()
rejectConnection console frameHandler message = let response = errorFrame message in do  
    log console "Rejecting connection!"
    put frameHandler response
    close frameHandler

-- |Given a CONNECT Frame, determine the highest common supported version of STOMP.
determineVersion :: Frame -> Maybe String
determineVersion frame = 
    let clientVersions = getSupportedVersions frame in
        case clientVersions of 
            Just versions -> getHighestSupportedVersion versions
            Nothing       -> Nothing

-- |Given a list of client protocol versions, determine the highest version supported
-- by both the server and the client.
getHighestSupportedVersion :: [String] -> Maybe String
getHighestSupportedVersion clientVersions = 
    let mutualVersions = intersect clientVersions supportedVersions in
        maybeMax mutualVersions

-- |Loop, receiving and processing new Frames from the client.
connectionLoop :: FrameHandler -> Logger -> SubscriptionManager -> ClientId -> [Subscription] -> IO ()
connectionLoop frameHandler console subManager clientId subs = do
    result <- try (handleNextFrame frameHandler console subManager clientId subs) 
        :: IO (Either SomeException (Maybe Command, [Subscription]))
    case result of
        Left exception -> do
            log console $ "There was an error processing a client frame: " ++ (show exception)
            rejectConnection console frameHandler ("Error: " ++ (show exception))
        Right (command, subs)-> case command of
            Just DISCONNECT -> return ()
            Just _          -> connectionLoop frameHandler console subManager clientId subs
            Nothing         -> do
                close frameHandler
                return ()

-- |This function blocks until a Frame is received from the client, and then processes that Frame appropriately.
handleNextFrame :: FrameHandler -> Logger -> SubscriptionManager -> ClientId -> [Subscription] -> IO (Maybe Command, [Subscription])
handleNextFrame frameHandler console subManager clientId subs = do 
    f <- get frameHandler
    case f of 
        NewFrame frame -> do
            command <- return $ getCommand frame
            log console $ "Received " ++ (show command) ++ " frame"
            handleReceiptRequest frameHandler frame console
            case command of
                DISCONNECT -> do 
                    log console "Disconnect request received; closing connection to client"
                    close frameHandler
                    return (Just command, subs)
                SEND       -> do 
                    handleSendFrame frame console subManager
                    return (Just command, subs)
                SUBSCRIBE  -> do
                    newSub <- handleSubscriptionRequest frameHandler frame subManager clientId
                    return (Just command, newSub:subs)
                _          -> do
                    log console "Handler not yet implemented"
                    return (Just command, subs)
        GotEof         -> do
            log console "Client disconnected without sending a frame."
            return (Nothing, subs)
        ParseError msg -> do
            log console $ "There was an error parsing a client frame: " ++ (show msg)
            return (Nothing, subs)

-- |Notify the SubscriptionManager that a new SEND Frame was received
handleSendFrame :: Frame -> Logger -> SubscriptionManager -> IO ()
handleSendFrame frame console subManager = reportMessage subManager frame

-- |Notify the SubscriptionManager that a new SUBSCRIBE Frame was received
handleSubscriptionRequest :: FrameHandler -> Frame -> SubscriptionManager -> ClientId -> IO Subscription
handleSubscriptionRequest handler frame subManager clientId = 
    let (maybeDest, maybeId) = (getDestination frame, getId frame) in
        getNewSub maybeDest maybeId handler subManager clientId

-- |Helper function for handleSubscriptionRequest
getNewSub :: Maybe String -> Maybe String -> FrameHandler -> SubscriptionManager -> ClientId -> IO Subscription
getNewSub Nothing _ _ _ _ = throw NoDestinationHeader
getNewSub _ Nothing _ _ _ = throw NoIdHeader
getNewSub (Just dest) (Just subId) handler subManager clientId = 
    addSubscription subManager clientId handler subId dest

-- Given a Frame, if the Frame contains a receipt Header, send a RECEIPT Frame to the client
handleReceiptRequest :: FrameHandler -> Frame -> Logger -> IO ()
handleReceiptRequest frameHandler frame console = do
    case (getReceipt frame) of
        Just receiptId -> do
            log console $ "Sending receipt for message with receipt ID: " ++ receiptId
            sendReceipt frameHandler receiptId
        _ -> return ()

-- |Helper function for handleReceiptRequest
sendReceipt :: FrameHandler -> String -> IO ()
sendReceipt frameHandler receiptId = do
    put frameHandler $ receipt receiptId

-- |Utility function for finding the maximal element in a list of ordinal values. Returns Nothing if the list is empty.
maybeMax :: Ord a => [a] -> Maybe a
maybeMax [] = Nothing
maybeMax xs = Just (List.maximum xs)

-- |Supported protocol versions for this server
supportedVersions :: [String]
supportedVersions = ["1.2"]

-- |Generate a comma-separated String of the supported versions
supportedVersionsAsString :: String
supportedVersionsAsString = List.intercalate ", " supportedVersions

appendTransform :: String -> IO String -> IO String
appendTransform string ioString = do
    s' <- ioString
    return $ string ++ " " ++ s'
