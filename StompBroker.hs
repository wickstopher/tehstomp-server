import Control.Concurrent
import Network.Socket
import System.IO

main :: IO ()
main = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 2323 iNADDR_ANY)
    listen sock 5
    putStrLn "STOMP broker initiated on port 2323"
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
    input <- hGetLine handle
    putStrLn input
    handleConnection handle
