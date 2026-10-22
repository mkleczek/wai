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
import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, threadDelay)
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

    xfail "implementation follows in the paired change" $ it "keeps idle streaming connections free of HTTP output payload storage" $
        bracket (socket AF_INET Stream defaultProtocol) close $ \sock ->
            bracket (socketConnectionHTTP1 defaultSettings sock)
                (\conn -> readIORef (connWriteBuffer conn) >>= bufFree) $ \conn -> do
                    scratch <- readIORef (connWriteBuffer conn)
                    bufSize scratch `shouldSatisfy` P.eq 0

    xfail "implementation follows in the paired change" $ it "lazily supports materialized compatibility responses" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withServer resources (\_ exchange -> respondResponse exchange (responseLBS status200 [] "owned")) $ \client -> do
                NS.sendAll client "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                response <- receiveAll client
                B.isInfixOf "owned" response `shouldSatisfy` P.eq True


getAcquisition :: IO Context
getAcquisition = newContext <$> getSystemTimerManager
