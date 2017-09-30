import Control.Concurrent
import Data.ByteString as BS
import Data.ByteString.UTF8 as UTF
import Data.ByteString.Char8 as Char8
import Data.List.Split
import Data.Word
import Network.Socket
import Stomp.Frames
import System.IO as IO

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
    forkIO (handleConnection handle)
    socketLoop sock

handleConnection :: Handle -> IO ()
handleConnection handle = do
    hSetBuffering handle NoBuffering
    processFrames handle

processFrames :: Handle -> IO ()
processFrames handle = do
    frame <- parseFrame handle
    IO.putStrLn $ show frame
    processFrames handle

parseFrame :: Handle -> IO Frame
parseFrame handle = do
    command <- getCommand handle
    headers <- getHeaders handle EndOfHeaders
    body    <- getBody handle (getContentLength headers)
    return $ Frame command headers body

getCommand :: Handle -> IO Command
getCommand handle = do
    commandLine <- BS.hGetLine handle
    return $ stringToCommand (toString commandLine)

getHeaders :: Handle -> Headers -> IO Headers
getHeaders handle headers = do
    line <- stringFromHandle handle
    if line == "" then
        return headers
    else
        getHeaders handle (addHeaderEnd (headerFromLine line) headers)

getBody :: Handle -> Maybe Int -> IO Body
getBody handle (Just n) = do
    bytes <- hGet handle n
    nullByte <- hGet handle 1
    return (Body bytes)
getBody handle Nothing  = do 
    bytes <- hGet handle 1
    if bytes == (fromString "\NUL") then
        return EmptyBody
    else do
        body <- getBodyNoContentLengthHeader handle [BS.head bytes]
        return $ Body body

getBodyNoContentLengthHeader :: Handle -> [Word8] -> IO ByteString
getBodyNoContentLengthHeader handle bytes = do
    byte <- hGet handle 1
    if byte == (fromString "\NUL") then
        return (BS.pack $ Prelude.reverse bytes)
    else
        getBodyNoContentLengthHeader handle ((BS.head byte) : bytes)

tokenize :: String -> String -> [String]
tokenize delimiter = Prelude.filter (not . Prelude.null) . splitOn delimiter

headerFromLine :: String -> Header
headerFromLine line = let tokens = tokenize ":" line in
    Header (Prelude.head tokens) (Prelude.last tokens)

stringFromHandle :: Handle -> IO String
stringFromHandle handle = do
    line <- BS.hGetLine handle
    return $ toString line