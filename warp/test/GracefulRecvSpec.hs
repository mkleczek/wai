{-# LANGUAGE OverloadedStrings #-}

module GracefulRecvSpec (spec) where

import Control.Concurrent.STM (STM, newTVarIO, retry)
import Control.Exception (bracket)
import Control.Monad (void)
import qualified Data.ByteString as S
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (castPtr)
import Network.Socket (
    Family (AF_INET),
    SockAddr (SockAddrInet),
    Socket,
    SocketType (Stream),
    accept,
    bind,
    close,
    connect,
    defaultProtocol,
    getSocketName,
    listen,
    socket,
    tupleToHostAddress,
    withReadable,
 )
import Network.Socket.ByteString (sendAll)
import Network.Wai.Handler.Warp.Run (makeGracefulRecvInto)
import Network.Wai.Handler.Warp.Settings (ServerState (..), newServerState)
import Network.Wai.Handler.Warp.ShuttingDown (writeShuttingDown)
import System.Timeout (timeout)
import Test.Hspec

-- | A connected pair of sockets: the one the server receives on and its peer.
connectedPair :: IO (Socket, Socket)
connectedPair = bracket listener close $ \sock -> do
    address <- getSocketName sock
    peer <- socket AF_INET Stream defaultProtocol
    connect peer address
    (local, _) <- accept sock
    return (local, peer)
  where
    listener = do
        sock <- socket AF_INET Stream defaultProtocol
        bind sock (SockAddrInet 0 (tupleToHostAddress (127, 0, 0, 1)))
        listen sock 1
        return sock

-- | Returns once the socket has bytes to read.
waitReadable :: Socket -> IO ()
waitReadable sock = void $ withReadable sock (retry :: STM ()) (return ())

-- | Receives on one end of a connected pair, with the graceful shutdown of
--   the server already reached and no application using the socket, so the
--   receive's give-up is satisfied before it starts.
withShutdownReceive :: (Socket -> Socket -> IO (Maybe S.ByteString) -> IO a) -> IO a
withShutdownReceive action = do
    ss <- newServerState
    writeShuttingDown (serverShuttingDown ss) True
    appsInProgress <- newTVarIO 0
    bracket connectedPair (\(local, peer) -> close local >> close peer) $ \(local, peer) ->
        allocaBytes 16 $ \ptr -> do
            let receive = timeout 2000000 $ do
                    n <- makeGracefulRecvInto local ss appsInProgress ptr 16
                    S.packCStringLen (castPtr ptr, n)
            action local peer receive

spec :: Spec
spec = describe "makeGracefulRecvInto" $ do
    describe "when the graceful shutdown is already reached" $ do
        it "returns the bytes already waiting on the socket" $
            withShutdownReceive $ \local peer receive -> do
                sendAll peer "hello"
                -- The bytes are in the socket before the receive starts.
                -- Waiting for readability keeps this independent of how
                -- fast the loopback delivers them.
                timeout 2000000 (waitReadable local) `shouldReturn` Just ()
                receive `shouldReturn` Just "hello"
        it "gives up at once when nothing is waiting" $
            withShutdownReceive $ \_ _ receive ->
                receive `shouldReturn` Just ""
