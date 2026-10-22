{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module RunnerSpec (spec) where

import Control.Concurrent.Resource.Acquisition (Context, newContext)
import GHC.Event (getSystemTimerManager)
import Control.Exception (bracket, evaluate)
import System.Timeout (timeout)
import Foreign.ForeignPtr (withForeignPtr, mallocForeignPtrBytes)
import Foreign.Marshal.Utils (fillBytes)
import Network.Socket (socket, Family (AF_INET), SocketType (Stream), defaultProtocol, close, SockAddr (SockAddrInet), tupleToHostAddress)
import Network.Wai.Handler.Warp (defaultSettings, setTimeout, setProxyProtocolRequired, setProxyProtocolOptional)
import Network.Wai.Handler.Warp.Internal (socketConnectionHTTP1, connWriteBuffer, bufSize, bufFree)
import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, threadDelay, readMVar)
import Control.Concurrent.Async (Async, withAsync, wait)
import qualified Network.Socket as Socket
import qualified Network.Wai.Handler.Warp as Warp
import qualified Network.Wai.Handler.Warp.Streaming as WarpStreaming
import Data.IORef
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Network.HTTP.Types (status200)
import qualified Network.Socket.ByteString as NS
import Network.Streaming (Source (..))
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Streaming.Buffer (Buffer (..))
import Network.Wai (rawPathInfo, remoteHost, responseLBS)
import qualified Network.Wai.Streaming as S
import Network.Wai.Handler.Warp.Streaming.Internal (withBodyBuffer, respondResponse)
import RunnerSupport
import Skeletest
import qualified Skeletest.Predicate as P

pull :: Source -> IO B.ByteString
pull source = withChunk source (evaluate . B.copy)

spec :: Spec
spec = describe "HTTP/1 operation-stream runner" $ do
    it "shuts down an idle connection before its first request" $ do
        resources <- shutdownResources
        withShutdownServer resources replyOk $ \_ shutdown server -> do
            shutdown
            timeout 1000000 (wait server) `shouldSatisfy` P.returns (P.eq (Just ()))

    it "shuts down an idle keep-alive connection after its response" $ do
        resources <- shutdownResources
        withShutdownServer resources replyOk $ \client shutdown server -> do
            NS.sendAll client "GET / HTTP/1.1\r\nHost: local\r\n\r\n"
            response <- receiveReply client ""
            B.isSuffixOf "ok" response `shouldSatisfy` P.eq True
            shutdown
            timeout 1000000 (wait server) `shouldSatisfy` P.returns (P.eq (Just ()))

    it "finishes an active upload while shutting down" $ do
        resources <- shutdownResources
        entered <- newEmptyMVar
        let app _ exchange = S.withBody exchange $ \source -> do
                putMVar entered ()
                pull source `shouldSatisfy` P.returns (P.eq "abc")
                S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
        withShutdownServer resources app $ \client shutdown server -> do
            NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\n\r\n"
            takeMVar entered
            shutdown
            timeout 100000 (wait server) `shouldSatisfy` P.returns (P.eq Nothing)
            NS.sendAll client "abc"
            response <- receiveAll client
            B.isSuffixOf "ok" response `shouldSatisfy` P.eq True
            timeout 1000000 (wait server) `shouldSatisfy` P.returns (P.eq (Just ()))

    it "waits for an active response when shutting down" $ do
        resources <- shutdownResources
        entered <- newEmptyMVar
        release <- newEmptyMVar
        let app _ exchange = S.respond exchange status200 [("Content-Length", "2")] $ \sink -> do
                sink "o"
                putMVar entered ()
                readMVar release
                sink "k"
        withShutdownServer resources app $ \client shutdown server -> do
            NS.sendAll client "GET / HTTP/1.1\r\nHost: local\r\n\r\n"
            takeMVar entered
            shutdown
            timeout 100000 (wait server) `shouldSatisfy` P.returns (P.eq Nothing)
            putMVar release ()
            response <- receiveAll client
            B.isSuffixOf "ok" response `shouldSatisfy` P.eq True
            timeout 1000000 (wait server) `shouldSatisfy` P.returns (P.eq (Just ()))

    it "continues expected uploads before draining an unpulled body scope" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app _ exchange = do
                    S.withBody exchange (\_ -> pure ())
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
                receiveInterim client accumulated = do
                    chunk <- NS.recv client 4096
                    let bytes = accumulated <> chunk
                    if B.isSuffixOf "\r\n\r\n" bytes || B.null chunk then pure bytes
                        else receiveInterim client bytes
            withServer resources app $ \client -> do
                NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nExpect: 100-continue\r\nContent-Length: 3\r\nConnection: close\r\n\r\n"
                interim <- timeout 1000000 (receiveInterim client "")
                interim `shouldSatisfy` P.eq (Just "HTTP/1.1 100 Continue\r\n\r\n")
                NS.sendAll client "abc"
                response <- receiveAll client
                B.isSuffixOf "ok" response `shouldSatisfy` P.eq True
    it "receives post-header bytes directly into the admitted input buffer" $ do
        fp <- mallocForeignPtrBytes 8
        selected <- newEmptyMVar
        observed <- newEmptyMVar
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app _ exchange = withBodyBuffer exchange (Buffer fp 8) $ \source -> do
                    putMVar selected ()
                    withChunk source $ \chunk -> do
                        let (actual, _, _) = BI.toForeignPtr chunk
                        owned <- evaluate (B.copy chunk)
                        putMVar observed (actual == fp, owned)
                        S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink chunk)
            withServer resources app $ \client -> do
                NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\n"
                takeMVar selected
                NS.sendAll client "abc"
                response <- receiveAll client
                B.isSuffixOf "abc" response `shouldSatisfy` P.eq True
            takeMVar observed `shouldSatisfy` P.returns (P.eq (True, "abc"))
    it "drains unread bodies and preserves one bounded pipeline prefix across buffer reuse" $ do
        count <- newIORef (0 :: Int)
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app req exchange = do
                    modifyIORef' count (+1)
                    let body = if rawPathInfo (S.requestMetadata req) == "/one" then "one" else "two"
                    S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink body)
            withServer resources app $ \client -> do
                NS.sendAll client "POST /one HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\n\r\nabcGET /two HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                response <- receiveAll client
                B.isInfixOf "\r\n\r\noneHTTP/1.1 200" response `shouldSatisfy` P.eq True
                B.isSuffixOf "two" response `shouldSatisfy` P.eq True
            readIORef count `shouldSatisfy` P.returns (P.eq 2)

    it "keeps idle streaming connections free of HTTP output payload storage" $
        bracket (socket AF_INET Stream defaultProtocol) close $ \sock ->
            bracket (socketConnectionHTTP1 defaultSettings sock)
                (\conn -> readIORef (connWriteBuffer conn) >>= bufFree) $ \conn -> do
                    scratch <- readIORef (connWriteBuffer conn)
                    bufSize scratch `shouldSatisfy` P.eq 0

    it "lazily supports materialized compatibility responses" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withServer resources (\_ exchange -> respondResponse exchange (responseLBS status200 [] "owned")) $ \client -> do
                NS.sendAll client "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                response <- receiveAll client
                B.isInfixOf "owned" response `shouldSatisfy` P.eq True

    it "pauses the transport timeout after withBody returns to application code" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app _ exchange = do
                    S.withBody exchange $ \source -> pull source `shouldSatisfy` P.returns (P.eq "abc")
                    threadDelay 1500000
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            withServerSettings (setTimeout 1 defaultSettings) resources app $ \client -> do
                NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"
                response <- receiveAll client
                B.isSuffixOf "ok" response `shouldSatisfy` P.eq True

    it "owns borrowed chunked pipeline framing before the input buffer is poisoned" $ do
        fp <- mallocForeignPtrBytes 8
        selected <- newEmptyMVar
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app req exchange
                    | rawPathInfo (S.requestMetadata req) == "/one" = do
                        withBodyBuffer exchange (Buffer fp 8) $ \_ -> do
                            putMVar selected ()
                            S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink "one")
                        withForeignPtr fp $ \ptr -> fillBytes ptr 88 8
                    | otherwise = S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink "two")
            withServer resources app $ \client -> do
                NS.sendAll client "POST /one HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n"
                takeMVar selected
                NS.sendAll client "3\r\nabc\r\n0\r\n\r\nGET /two HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                response <- receiveAll client
                B.isInfixOf "\r\n\r\noneHTTP/1.1 200" response `shouldSatisfy` P.eq True
                B.isSuffixOf "two" response `shouldSatisfy` P.eq True
    it "times out idle keep-alive header reads without holding body storage" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
                let app _ exchange = S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
                    receiveReply client accumulated = do
                        chunk <- NS.recv client 4096
                        let result = accumulated <> chunk
                        if B.isSuffixOf "ok" result || B.null chunk then pure result
                            else receiveReply client result
                withServerSettings (setTimeout 1 defaultSettings) resources app $ \client -> do
                    NS.sendAll client "GET / HTTP/1.1\r\nHost: local\r\n\r\n"
                    response <- receiveReply client ""
                    B.isSuffixOf "ok" response `shouldSatisfy` P.eq True
                    timeout 300000 (withRequestBuffers resources (\_ -> pure ())) `shouldSatisfy` P.returns (P.eq (Just ()))
                    receiveAll client `shouldSatisfy` P.returns (P.eq "")

    it "parses PROXY control metadata before admitting request payload storage" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app request exchange = do
                    remoteHost (S.requestMetadata request) `shouldSatisfy` P.eq (SockAddrInet 1234 (tupleToHostAddress (1,2,3,4)))
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            mapM_ (\settings -> withServerSettings settings resources app $ \client -> do
                NS.sendAll client "PROXY TCP4 1.2.3.4 127.0.0.1 1234 80\r\nGET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                response <- receiveAll client
                B.isSuffixOf "ok" response `shouldSatisfy` P.eq True)
                [setProxyProtocolRequired defaultSettings, setProxyProtocolOptional defaultSettings]

getAcquisition :: IO Context
getAcquisition = newContext <$> getSystemTimerManager

shutdownResources :: IO BufferPool
shutdownResources = getAcquisition >>= \acquisition -> newBufferPool acquisition
    (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 })

replyOk :: S.Application
replyOk _ exchange = S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")

receiveReply :: Socket.Socket -> B.ByteString -> IO B.ByteString
receiveReply client accumulated = do
    chunk <- NS.recv client 4096
    let bytes = accumulated <> chunk
    if B.isSuffixOf "ok" bytes || B.null chunk then pure bytes else receiveReply client bytes

-- Success requires natural server completion after its installed shutdown
-- action; withAsync cancellation only cleans up a failed/timed-out assertion.
withShutdownServer :: BufferPool -> S.Application -> (Socket.Socket -> IO () -> Async () -> IO a) -> IO a
withShutdownServer resources app action = do
    shutdown <- newEmptyMVar
    opened <- newEmptyMVar
    let settings = Warp.setInstallShutdownHandler (putMVar shutdown)
            $ Warp.setOnOpen (\_ -> putMVar opened () >> pure True)
            $ Warp.setGracefulCloseTimeout1 0 Warp.defaultSettings
        listener = do
            sock <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
            Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127,0,0,1)))
            Socket.listen sock 10
            pure sock
    bracket listener Socket.close $ \sock -> do
        address <- Socket.getSocketName sock
        withAsync (WarpStreaming.runHTTP1Socket settings resources sock app) $ \server ->
            bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \client -> do
                Socket.connect client address
                takeMVar opened
                closeListener <- takeMVar shutdown
                result <- timeout 5000000 (action client closeListener server)
                maybe (fail "graceful streaming shutdown test timed out") pure result
