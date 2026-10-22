{-# LANGUAGE OverloadedStrings #-}
module RunnerSupport (withServer, withServerSettings, withServerSettingsAt, receiveAll) where

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
withServer = withServerSettings defaultSettings

withServerSettings :: Settings -> BufferPool -> Streaming.Application -> (Socket -> IO a) -> IO a
withServerSettings settings resources app action =
    withServerSettingsAt settings resources app $ \address ->
        bracket (socket AF_INET Stream defaultProtocol) close $ \client -> do
            connect client address
            action client

withServerSettingsAt :: Settings -> BufferPool -> Streaming.Application -> (SockAddr -> IO a) -> IO a
withServerSettingsAt settings resources app action = bracket listener close $ \sock -> do
    address <- getSocketName sock
    withAsync (runHTTP1Socket (setGracefulCloseTimeout1 0 settings) resources sock app) $ \server -> do
        result <- timeout 5000000 (action address)
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
