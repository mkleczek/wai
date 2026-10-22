{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module ExchangeSpec (spec) where

import Control.Concurrent.Resource.Acquisition (Context, newContext)
import GHC.Event (getSystemTimerManager)
import Control.Monad (void)
import Control.Exception (SomeException, try, finally, evaluate)
import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, tryPutMVar, threadDelay)
import Control.Concurrent.Async (async, waitCatch, cancel)
import System.Timeout (timeout)
import Data.Maybe (isJust)
import Data.Either (isLeft)
import Data.IORef
import Foreign.ForeignPtr (mallocForeignPtrBytes)
import Network.HTTP.Types (http11, http20, status200)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Streaming.Buffer (Buffer (..))
import qualified Network.Wai
import Network.Wai (defaultRequest, httpVersion, getRequestBodyChunk, responseLBS, responseRaw)
import Network.Wai.Internal (Response (..))
import qualified Network.Wai.Streaming as S
import Network.Wai.Handler.Warp.Streaming.Internal hiding (withHTTP1Exchange)
import qualified Network.Wai.Handler.Warp.Streaming.Internal as Internal
import Network.Streaming (Source (..))
import qualified Data.ByteString as B
import qualified Skeletest.Predicate as P
import Skeletest

owned :: IO B.ByteString -> Source
owned action = Source $ \consume -> action >>= consume

pull :: Source -> IO B.ByteString
pull source = withChunk source (evaluate . B.copy)

withHTTP1Exchange :: BufferPool -> Network.Wai.Request
    -> (forall b. Buffer -> (Source -> IO b) -> IO b)
    -> (Network.Wai.Response -> IO ())
    -> (S.Request -> S.Exchange -> IO a) -> IO a
withHTTP1Exchange resources request attach send app =
    withRequestBuffers resources $ \access ->
        Internal.withHTTP1Exchange access request
            (\supplied use -> case supplied of
                Just buffer -> attach buffer use
                Nothing -> withBuffer access (`attach` use)) send app

rejects :: IO a -> IO ()
rejects action = do
    outcome <- try (void action) :: IO (Either SomeException ())
    isLeft outcome `shouldSatisfy` P.eq True

spec :: Spec
spec = describe "HTTP/1 operation exchange" $ do
    it "uses explicitly supplied storage in the low-level input hook" $ do
        fp <- mallocForeignPtrBytes 8
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\buffer use -> do
                    bufferMemory buffer `shouldSatisfy` P.eq fp
                    use (owned $ pure "borrowed"))
                (\_ -> pure ()) $ \request exchange -> do
                    rejects (getRequestBodyChunk (S.requestMetadata request))
                    withBodyBuffer exchange (Buffer fp 8) $ \source -> do
                        pull source `shouldSatisfy` P.returns (P.eq "borrowed")
                        withChunk source $ \_ -> rejects (pull source)
                        rejects (withBodyBuffer exchange (Buffer fp 8) (\_ -> pure ()))
                    rejects (withBodyBuffer exchange (Buffer fp 8) (\_ -> pure ()))
                    S.respond exchange status200 [] (\_ -> pure ())
    it "expires retained sources, sinks, and the exchange" $ do
        fp <- mallocForeignPtrBytes 8
        sourceRef <- newIORef (owned $ pure "")
        sinkRef <- newIORef (\_ -> pure ())
        exchangeRef <- newIORef Nothing
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\_ use -> use (owned $ pure "x"))
                (\case
                    ResponseBytes _ _ body -> body (\_ -> pure ())
                    _ -> failTest "expected direct-byte response") $ \_ exchange -> do
                    writeIORef exchangeRef (Just exchange)
                    withBodyBuffer exchange (Buffer fp 8) $ \source -> writeIORef sourceRef source
                    readIORef sourceRef >>= (rejects . pull)
                    S.respond exchange status200 [] $ \sink -> writeIORef sinkRef sink
                    readIORef sinkRef >>= (\sink -> rejects (sink "late"))
                    rejects (S.respond exchange status200 [] (\_ -> pure ()))
            Just exchange <- readIORef exchangeRef
            rejects (S.respond exchange status200 [] (\_ -> pure ()))
            rejects (withBodyBuffer exchange (Buffer fp 8) (\_ -> pure ()))
    it "rejects HTTP/2 before using backend capabilities" $ do
        called <- newIORef False
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\_ use -> use (owned $ pure "")) (\_ -> pure ())
                (\_ exchange -> S.respond exchange status200 [] (\_ -> pure ()))
            rejects $ withHTTP1Exchange resources (defaultRequest { httpVersion = http20 })
                (\_ use -> writeIORef called True >> use (owned $ pure ""))
                (\_ -> writeIORef called True) (\_ _ -> writeIORef called True)
            readIORef called `shouldSatisfy` P.returns (P.eq False)

    it "public withBody uses one pooled input region and returns it at scope exit" $ do
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\buffer use -> do
                    bufferCapacity buffer `shouldSatisfy` P.eq 8
                    use (owned $ pure "pooled")) (\_ -> pure ()) $ \request exchange -> do
                    S.withBody exchange $ \source -> pull source `shouldSatisfy` P.returns (P.eq "pooled")
                    withBuffer (S.requestBuffers request) $ \buffer -> bufferCapacity buffer `shouldSatisfy` P.eq 8
                    S.respond exchange status200 [] (\_ -> pure ())
    it "rejects concurrent pulls and joins a blocked reader before input detach" $ do
        fp <- mallocForeignPtrBytes 8
        started <- newEmptyMVar
        blocked <- newEmptyMVar
        detached <- newIORef False
        sawDetached <- newIORef True
        workerRef <- newIORef Nothing
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\_ use -> use
                    (owned $ (putMVar started () >> takeMVar blocked >> pure "")
                        `finally` (readIORef detached >>= writeIORef sawDetached))
                    `finally` writeIORef detached True)
                (\_ -> pure ()) $ \_ exchange -> do
                    rejects $ withBodyBuffer exchange (Buffer fp 8) $ \source -> do
                        worker <- async (pull source)
                        writeIORef workerRef (Just worker)
                        takeMVar started
                        rejects (pull source)
                    Just worker <- readIORef workerRef
                    joined <- timeout 1000000 (waitCatch worker)
                    isJust joined `shouldSatisfy` P.eq True
                    readIORef sawDetached `shouldSatisfy` P.returns (P.eq False)
                    S.respond exchange status200 [] (\_ -> pure ())

    it "rejects concurrent sinks and joins blocked writes before responding returns" $ do
        started <- newEmptyMVar
        blocked <- newEmptyMVar
        workerRef <- newIORef Nothing
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\_ use -> use (owned $ pure ""))
                (\case
                    ResponseBytes _ _ body -> body (\_ -> putMVar started () >> takeMVar blocked)
                    _ -> failTest "expected direct-byte response") $ \_ exchange -> do
                    rejects $ S.respond exchange status200 [] $ \sink -> do
                        worker <- async (sink "first")
                        writeIORef workerRef (Just worker)
                        takeMVar started
                        rejects (sink "second")
                    Just worker <- readIORef workerRef
                    joined <- timeout 1000000 (waitCatch worker)
                    isJust joined `shouldSatisfy` P.eq True

    it "routes ordinary responses through the same one-shot gate and rejects raw upgrades" $ do
        sent <- newIORef False
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withHTTP1Exchange resources (defaultRequest { httpVersion = http11 })
                (\_ use -> use (owned $ pure "")) (\_ -> writeIORef sent True) $ \_ exchange -> do
                    rejects (respondResponse exchange (responseRaw (\_ _ -> pure ()) (responseLBS status200 [] "")))
                    respondResponse exchange (responseLBS status200 [] "owned")
                    rejects (S.respond exchange status200 [] (\_ -> pure ()))
                    readIORef sent `shouldSatisfy` P.returns (P.eq True)


getAcquisition :: IO Context
getAcquisition = newContext <$> getSystemTimerManager
