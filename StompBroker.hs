import Control.Concurrent
import Control.Concurrent.TxEvent
import Control.Exception
import Data.ByteString as BS
import Data.List as List
import Stomp.Frames.IO
import Network.Socket hiding (close)
import Prelude hiding (log)
import System.IO as IO
import System.Environment
import Stomp.Frames hiding (subscribe, unsubscribe, begin, commit, abort)
import Stomp.Increment
import Stomp.TLogger
import Stomp.Subscriptions
import Stomp.Transaction as Transaction

-- |ClientExceptions are thrown during frame processing
data ClientException = NoIdHeader |
                       NoDestinationHeader  |
                       SubscriptionUpdate |
                       NoIdInUnsubscribe | 
                       InvalidTransactionHeader Command |
                       TransactionException String

-- |Encapsulation of a single client's connection data
data Connection      = Connection ClientId FrameHandler ClientTransactionManager Int

instance Exception ClientException
instance Show ClientException where
    show NoIdHeader                   = "No id header present in request"
    show NoDestinationHeader          = "No destination header present in request"
    show SubscriptionUpdate           = "There was an error processing the subscription request"
    show NoIdInUnsubscribe            = "No subscription ID header present in unsubscribe request"
    show (InvalidTransactionHeader c) = "Invalid transaction header in " ++ (show c) ++ " frame"
    show (TransactionException s)     = "Error occurred while processing transaction: " ++ s

-- |Set up the environment and initialize the server loop.
main :: IO ()
main = do
    args         <- getArgs
    port         <- processArgs args
    console      <- dateTimeLogger stdout
    subManager   <- initManager
    sock         <- socket AF_INET Stream 0
    incrementer  <- newIncrementer
    frameCounter <- newIncrementer
    initFrameCounter frameCounter
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet port iNADDR_ANY)
    listen sock 5
    log console $ "STOMP broker initiated on port " ++ (show port)
    socketLoop sock console subManager incrementer frameCounter

-- |Process the command-line arguments.
processArgs :: [String] -> IO PortNumber
processArgs (s:[]) = return $ fromIntegral ((read s)::Int)
processArgs _      = return 2323

-- |Loop as connections are received, forking off a new thread for each connection.
socketLoop :: Socket -> Logger -> SubscriptionManager -> Incrementer -> Incrementer -> IO ()
socketLoop sock console subManager inc frameCounter = do
    (uSock, _) <- accept sock
    addr <- getPeerName uSock
    log console $ "New connection received from " ++ (show addr)
    handle <- socketToHandle uSock ReadWriteMode
    hSetBuffering handle NoBuffering
    frameHandler <- initFrameHandler handle
    forkIO $ negotiateConnection frameHandler (addTransform (appendTransform ("[" ++ show addr ++ "]")) console) subManager inc frameCounter
    socketLoop sock console subManager inc frameCounter

initFrameCounter :: Incrementer -> IO ThreadId
initFrameCounter inc = do
    fileHandle <- openFile "frameCount.log" AppendMode
    hSetBuffering fileHandle NoBuffering
    logger     <- dateTimeLogger fileHandle
    forkIO $ frameCountLoop inc logger 0

frameCountLoop :: Incrementer -> Logger -> Integer -> IO ()
frameCountLoop inc logger lastCount = do
    currentCount <- sync $ timeOutEvt 1000000 `thenEvt` (\_ -> getLastEvt inc)
    log logger $ show (currentCount - lastCount)
    frameCountLoop inc logger currentCount
    
-- |Negotiate client connection. 
negotiateConnection :: FrameHandler -> Logger -> SubscriptionManager -> Incrementer -> Incrementer -> IO ()
negotiateConnection frameHandler console subManager inc frameCounter = do
    frameEvt <- get frameHandler
    case frameEvt of 
        NewFrame frame -> case (getCommand frame) of
            STOMP   -> do
                log console "STOMP frame received; negotiating new connection"
                handleNewConnection frameHandler frame console subManager inc frameCounter
            CONNECT -> do
                log console "CONNECT frame received; negotiating new connection"
                handleNewConnection frameHandler frame console subManager inc frameCounter
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
handleNewConnection :: FrameHandler -> Frame -> Logger -> SubscriptionManager -> Incrementer -> Incrementer -> IO ()
handleNewConnection frameHandler frame console subManager inc frameCounter = let version = determineVersion frame in
    case version of
        Just v  -> do 
            (clientSend, serverSend) <- return $ determineHeartbeats frame    
            log console $ "Heartbeat negotations are (" ++ (show clientSend) ++ "," ++ (show serverSend) ++ ")"        
            sendConnectedResponse frameHandler v clientSend serverSend
            updateHeartbeat frameHandler (1000 * serverSend) -- convert milliseconds to microseconds
            clientId                 <- getNext inc
            transactionManager       <- initTransactionManager subManager
            log console $ "Connection initiated to client using STOMP protocol version " ++ v
            log console $ "Client unique ID is " ++ (show clientId)
            connectionLoop console subManager frameCounter $ Connection 
                clientId 
                frameHandler 
                transactionManager 
                (if clientSend == 0 then 0 else 1000 * (clientSend + 2000)) -- Give 2 seconds of leeway, convert milliseconds to microseconds
        Nothing -> do
            log console "No common protocol versions supported; rejecting connection"
            rejectConnection console frameHandler ("Supported STOMP versions are: " ++  supportedVersionsAsString)

-- |Heartbeat negotation given a CONNECT frame.
determineHeartbeats :: Frame -> (Int, Int)
determineHeartbeats frame = 
    let (x, y) = getHeartbeat frame in
        (if x == 0 then 0 else max x 2000, 
            if y == 0 then 0 else max y 2000)

-- |Helper function for handleNewConnection; sends the CONNECTED Frame
sendConnectedResponse :: FrameHandler -> String -> Int -> Int -> IO ()
sendConnectedResponse frameHandler version clientSend serverSend = 
    let response = connected version serverSend clientSend in
        put frameHandler response

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
connectionLoop :: Logger -> SubscriptionManager -> Incrementer -> Connection -> IO ()
connectionLoop console subManager frameCounter connection@(Connection clientId frameHandler _ _) = do
    result <- try (handleNextFrame console subManager connection) 
        :: IO (Either SomeException (Maybe Command))
    getNext frameCounter
    case result of
        Left exception -> do
            log console $ "There was an error processing a client frame: " ++ (show exception)
            rejectConnection console frameHandler ("Error: " ++ (show exception))
        Right command -> case command of
            Just DISCONNECT -> return ()
            Just _          -> connectionLoop console subManager frameCounter connection
            Nothing         -> do
                disconnectClient connection subManager
                return ()

-- |Disconnect a client
disconnectClient :: Connection -> SubscriptionManager -> IO UpdateResponse
disconnectClient (Connection clientId frameHandler transactionManager _) subManager = do
    close frameHandler
    clientDisconnected subManager clientId
    Transaction.disconnect transactionManager

-- |This function blocks until a Frame is received from the client, and then processes that Frame appropriately.
handleNextFrame :: Logger -> SubscriptionManager -> Connection -> IO (Maybe Command)
handleNextFrame console subManager connection@(Connection _ frameHandler _ clientSend) = do 
    frameEvt <- sync $ getEvtWithTimeOut frameHandler clientSend
    case frameEvt of 
        NewFrame frame -> do
            command <- return $ getCommand frame
            --log console $ "Received " ++ (show command) ++ " frame"
            handleReceiptRequest frameHandler frame console
            case (getTransaction frame) of
                Just txid -> handleTransactionFrame command frame txid console connection
                Nothing -> handleSingleFrame command frame console subManager connection
        Heartbeat      -> return $ Just SEND
        TimedOut       -> do
            log console "Timed out waiting for a heartbeat from the client"
            return Nothing
        GotEof         -> do
            log console "Client disconnected without sending a frame."
            return Nothing
        ParseError msg -> do
            log console $ "There was an error parsing a client frame: " ++ (show msg)
            return Nothing

-- |Handle a single frame that is not part of a transaction.
handleSingleFrame :: Command -> Frame -> Logger -> SubscriptionManager -> Connection -> IO (Maybe Command)
handleSingleFrame command frame console subManager connection@(Connection clientId frameHandler _ _ ) =
    case command of
        DISCONNECT  -> do 
            log console "Disconnect request received; closing connection to client"
            disconnectClient connection subManager
            return $ Just command
        SEND        -> do 
            handleSendFrame frame console subManager
            return $ Just command
        SUBSCRIBE   -> do
            handleSubscriptionRequest frameHandler frame subManager clientId
            return $ Just command
        UNSUBSCRIBE -> do
            handleUnsubscribeRequest subManager clientId frame
            return $ Just command
        ACK         -> do
            sendAckResponse subManager clientId frame
            return $ Just command
        NACK        -> do
            sendAckResponse subManager clientId frame
            return $ Just command
        _           -> do
            log console "Handler not yet implemented"
            return $ Just command

-- |Handle a single frame that is part of a transaction.
handleTransactionFrame :: Command -> Frame -> TransactionId -> Logger -> Connection -> IO (Maybe Command)
handleTransactionFrame command frame txid console connection@(Connection clientId _ transactionManager _) =
    let 
        execute = case command of
            BEGIN  -> begin txid transactionManager
            COMMIT -> commit txid transactionManager
            ABORT  -> abort txid transactionManager
            SEND   -> case getDestination frame of
                Just dest -> Transaction.send txid dest frame transactionManager
                Nothing   -> throw NoDestinationHeader
            ACK    -> ackResponse txid clientId frame transactionManager
            NACK   -> ackResponse txid clientId frame transactionManager
            _      -> throw $ InvalidTransactionHeader command
    in do
        log console ("Adding frame to transaction " ++ txid)
        response <- execute
        case response of
            Success -> return $ Just command
            Error s -> throw $ TransactionException s

-- |Notify the SubscriptionManager that a new SEND Frame was received
handleSendFrame :: Frame -> Logger -> SubscriptionManager -> IO ()
handleSendFrame frame console subManager = case getDestination frame of
    Just dest -> sendMessage subManager dest frame
    Nothing   -> throw NoDestinationHeader

-- |Notify the SubscriptionManager that a new SUBSCRIBE Frame was received
handleSubscriptionRequest :: FrameHandler -> Frame -> SubscriptionManager -> ClientId -> IO ()
handleSubscriptionRequest handler frame subManager clientId = 
    let maybeDest = getDestination frame
        maybeId   = getId frame
        ackType   = selectAckType (getAckType frame)
    in getNewSub maybeDest maybeId ackType handler subManager clientId

handleUnsubscribeRequest:: SubscriptionManager -> ClientId -> Frame -> IO ()
handleUnsubscribeRequest subManager clientId frame = case (getId frame) of 
    Just subId -> unsubscribe subManager clientId subId
    Nothing    -> throw NoIdInUnsubscribe

-- |Helper function for handleSubscriptionRequest
getNewSub :: Maybe String -> Maybe String -> AckType -> FrameHandler -> SubscriptionManager -> ClientId -> IO ()
getNewSub Nothing _ _ _ _ _ = throw NoDestinationHeader
getNewSub _ Nothing _ _ _ _ = throw NoIdHeader
getNewSub (Just dest) (Just subId) ackType handler subManager clientId = do
    subscribe subManager dest clientId subId ackType handler

-- |If no AckType is present, return Auto
selectAckType :: Maybe AckType -> AckType
selectAckType (Just ackType) = ackType
selectAckType Nothing        = Auto

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

-- |Log transform to append a string to all log messages
appendTransform :: String -> IO String -> IO String
appendTransform string ioString = do
    s' <- ioString
    return $ string ++ " " ++ s'
