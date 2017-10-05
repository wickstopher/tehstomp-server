import Control.Concurrent
import Data.ByteString as BS
import Data.List as List
import Stomp.Frames.IO
import Network.Socket
import Prelude hiding (log)
import System.IO as IO
import Stomp.Frames
import Stomp.TLogger

main :: IO ()
main = do
    console <- dateTimeLogger stdout
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 2323 iNADDR_ANY)
    listen sock 5
    log console "STOMP broker initiated on port 2323"
    socketLoop sock console

socketLoop :: Socket -> Logger -> IO ()
socketLoop sock console = do
    (uSock, _) <- accept sock
    addr <- getPeerName uSock
    log console $ "New connection received from " ++ (show addr)
    handle <- socketToHandle uSock ReadWriteMode
    forkIO $ negotiateConnection handle (addTransform (stringTransform ("[" ++ show addr ++ "]")) console)
    socketLoop sock console

negotiateConnection :: Handle -> Logger -> IO ()
negotiateConnection handle console = do
    hSetBuffering handle NoBuffering
    frame <- parseFrame handle
    case (getCommand frame) of
        STOMP   -> do
            log console "STOMP frame received; negotiating new connection"
            handleNewConnection handle frame console
        CONNECT -> do
            log console "CONNECT frame received; negotiating new connection"
            handleNewConnection handle frame console
        _       -> do
            log console $ (show $ getCommand frame) ++ " frame received; rejecting connection"
            rejectConnection handle "Please initiate communications with a connection request"
    
handleNewConnection :: Handle -> Frame -> Logger -> IO ()
handleNewConnection handle frame console = let version = determineVersion frame in
    case version of
        Just v  -> do 
            sendConnectedResponse handle v
            log console $ "Connection initiated to client using STOMP protocol version " ++ v
            connectionLoop handle console
        Nothing -> do
            log console "No common protocol versions supported; rejecting connection"
            rejectConnection handle ("Supported STOMP versions are: " ++  supportedVersionsAsString)

connectionLoop :: Handle -> Logger -> IO ()
connectionLoop handle console = do
    frame <- parseFrame handle
    log console $ "Received " ++ (show $ getCommand frame) ++ " frame"
    handleReceiptRequest handle frame console
    case (getCommand frame) of
        DISCONNECT -> do 
            log console "Disconnect request received; closing connection to client"
            hClose handle
            return ()
        _ -> connectionLoop handle console

sendConnectedResponse :: Handle -> String -> IO ()
sendConnectedResponse handle version = let response = connected version in
    do hPut handle $ frameToBytes response

rejectConnection :: Handle -> String -> IO ()
rejectConnection handle message = let response = errorFrame message in
    do  hPut handle $ frameToBytes response
        hClose handle

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

handleReceiptRequest :: Handle -> Frame -> Logger -> IO ()
handleReceiptRequest handle frame console = do
    case (getReceipt frame) of
        Just receiptId -> do
            log console $ "Sending receipt for message with receipt ID: " ++ receiptId
            sendReceipt handle receiptId
        _ -> return ()

sendReceipt :: Handle -> String -> IO ()
sendReceipt handle receiptId = do
    hPut handle $ frameToBytes (receipt receiptId)

stringTransform :: String -> IO String -> IO String
stringTransform string ioString = do
    s' <- ioString
    return $ string ++ " " ++ s'
