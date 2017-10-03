import Control.Concurrent
import Data.ByteString as BS
import Data.List as List
import Stomp.Frames.IO
import Network.Socket
import System.IO as IO
import Stomp.Frames

main :: IO ()
main = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 2323 iNADDR_ANY)
    listen sock 5
    IO.putStrLn "STOMP broker initiated on port 2323"
    socketLoop sock

socketLoop :: Socket -> IO ()
socketLoop sock = do
    (uSock, _) <- accept sock
    handle <- socketToHandle uSock ReadWriteMode
    forkIO (negotiateConnection handle)
    socketLoop sock

negotiateConnection :: Handle -> IO ()
negotiateConnection handle = do
    hSetBuffering handle NoBuffering
    frame <- parseFrame handle
    case (getCommand frame) of
        STOMP   -> handleNewConnection handle frame
        CONNECT -> handleNewConnection handle frame
        _       -> rejectConnection handle "Please initiate communications with a connection request"
    
handleNewConnection :: Handle -> Frame -> IO ()
handleNewConnection handle frame = let version = determineVersion frame in
    case version of
        Just v  -> do 
            sendConnectedResponse handle v
            connectionLoop handle
        Nothing -> rejectConnection handle ("Supported STOMP versions are: " ++  supportedVersionsAsString)

connectionLoop :: Handle -> IO ()
connectionLoop handle = do
    frame <- parseFrame handle
    handleReceiptRequest handle frame
    case (getCommand frame) of
        DISCONNECT -> do 
            hClose handle
            return ()
        _ -> connectionLoop handle

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

handleReceiptRequest :: Handle -> Frame -> IO ()
handleReceiptRequest handle frame = do
    case (getReceipt frame) of
        Just receiptId -> sendReceipt handle receiptId
        _ -> return ()


sendReceipt :: Handle -> String -> IO ()
sendReceipt handle receiptId = do
    hPut handle $ frameToBytes (receipt receiptId)
