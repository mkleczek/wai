{-# LANGUAGE OverloadedStrings #-}
module RunnerSupport (withServer, receiveAll) where

import Control.Concurrent.Async (withAsync, cancel)
import Control.Exception (bracket)
import qualified Data.ByteString as B
import Network.Socket
import qualified Network.Socket.ByteString as NS
import Network.Streaming.BufferPool (BufferPool)
import Network.Wai.Handler.Warp
import Network.Wai.Handler.Warp.Streaming
import qualified Network.Wai.Streaming as Streaming
import System.Timeout (timeout)

withServer :: BufferPool -> Streaming.Application -> (Socket -> IO a) -> IO a
withServer resources app action = bracket listener close $ \sock -> do
    address <- getSocketName sock
    withAsync (runHTTP1Socket (setGracefulCloseTimeout1 0 defaultSettings) resources sock app) $ \server ->
        bracket (socket AF_INET Stream defaultProtocol) close $ \client -> do
            connect client address
            result <- timeout 5000000 (action client)
            cancel server
            maybe (fail "operation-stream socket test timed out") pure result
  where
    listener = do
        sock <- socket AF_INET Stream defaultProtocol
        setSocketOption sock ReuseAddr 1
        bind sock (SockAddrInet 0 (tupleToHostAddress (127,0,0,1)))
        listen sock 10
        pure sock

receiveAll :: Socket -> IO B.ByteString
receiveAll sock = go []
  where
    go chunks = do
        bytes <- NS.recv sock 65536
        if B.null bytes then pure (B.concat (reverse chunks)) else go (bytes:chunks)
