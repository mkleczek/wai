{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
module RequestBuffersSpec (spec) where

import Control.Concurrent.Resource.Acquisition (Context, newContext)
import GHC.Event (getSystemTimerManager)
import Control.Concurrent (MVar, newEmptyMVar, putMVar, takeMVar, threadDelay, isEmptyMVar)
import Control.Concurrent.Async (AsyncCancelled, withAsync, wait, cancel, waitCatch)
import Control.Exception (bracket, fromException)
import Control.Monad (when)
import Data.IORef (newIORef, readIORef, writeIORef, modifyIORef')
import qualified Data.ByteString as B
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes, copyBytes)
import Foreign.Ptr (castPtr)
import Network.HTTP.Types (status200)
import Network.Socket (Socket, SockAddr (SockAddrInet), socket, connect, close, Family (AF_INET), SocketType (Stream), defaultProtocol)
import qualified Network.Socket.ByteString as NS
import Network.Streaming (withChunk)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Wai (rawPathInfo, requestHeaders)
import Network.Wai.Handler.Warp (defaultSettings, setTimeout)
import Network.Wai.Handler.Warp.Internal (Connection (..), Transport (TCP), serveHTTP1Connection)
import qualified Network.Wai.Streaming as S
import RunnerSupport
import OutputSpec (connection)
import Skeletest
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)

spec :: Spec
spec = describe "per-request body capabilities" $ do
    it "returns 503 and closes when the primary pool stays exhausted, without using the reserve" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            held <- newEmptyMVar
            releaseHeld <- newEmptyMVar
            called <- newEmptyMVar
            let app _ exchange = do
                    putMVar called ()
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            withAsync (withRequestBuffers resources $ \access -> putMVar held access >> takeMVar releaseHeld) $ \holder -> do
                access <- takeMVar held
                withServerSettingsAt (setTimeout 1 defaultSettings) resources app $ \address -> do
                    withClient address $ \client -> do
                        NS.sendAll client "GET /busy HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                        response <- receiveAll client
                        B.isPrefixOf "HTTP/1.1 503" response `shouldSatisfy` P.eq True
                        B.isInfixOf "Warp-Error: request-buffer-timeout\r\n" response `shouldSatisfy` P.eq True
                        B.isInfixOf "Content-Length: 0\r\n" response `shouldSatisfy` P.eq True
                        B.isInfixOf "Connection: close\r\n" response `shouldSatisfy` P.eq True
                        isEmptyMVar called `shouldSatisfy` P.returns (P.eq True)
                    -- A failure response needs no body buffer. The protected
                    -- reserve must remain available to the existing owner.
                    loan <- timeout 300000 (borrowBuffer access)
                    case loan of
                        Nothing -> fail "503 response borrowed the protected reserve"
                        Just buffer -> releaseBuffer buffer
                    putMVar releaseHeld ()
                    wait holder
                    -- The timed-out waiter must be detached from the FIFO.
                    withClient address $ \client -> do
                        NS.sendAll client "GET /after HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                        response <- receiveAll client
                        B.isPrefixOf "HTTP/1.1 200" response `shouldSatisfy` P.eq True

    it "admits a queued request when a primary buffer returns before its deadline" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            held <- newEmptyMVar
            releaseHeld <- newEmptyMVar
            called <- newEmptyMVar
            let app _ exchange = do
                    putMVar called ()
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            withAsync (withRequestBuffers resources $ \_ -> putMVar held () >> takeMVar releaseHeld) $ \holder -> do
                takeMVar held
                withServerSettings (setTimeout 1 defaultSettings) resources app $ \client -> do
                    NS.sendAll client "GET /ready HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                    threadDelay 200000
                    isEmptyMVar called `shouldSatisfy` P.returns (P.eq True)
                    putMVar releaseHeld ()
                    wait holder
                    response <- receiveAll client
                    B.isPrefixOf "HTTP/1.1 200" response `shouldSatisfy` P.eq True
                    B.isSuffixOf "ok" response `shouldSatisfy` P.eq True

    it "delivers 503 when a rejected upload has unread body bytes" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            held <- newEmptyMVar
            releaseHeld <- newEmptyMVar
            let app _ exchange = S.respond exchange status200 [] (\_ -> pure ())
            withAsync (withRequestBuffers resources $ \_ -> putMVar held () >> takeMVar releaseHeld) $ \holder -> do
                takeMVar held
                withServerSettings (setTimeout 1 defaultSettings) resources app $ \client -> do
                    NS.sendAll client "POST /busy HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"
                    response <- receiveAll client
                    B.isPrefixOf "HTTP/1.1 503" response `shouldSatisfy` P.eq True
                putMVar releaseHeld ()
                wait holder

    it "does not synthesize 503 after external cancellation of a queued connection" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            held <- newEmptyMVar
            releaseHeld <- newEmptyMVar
            received <- newEmptyMVar
            output <- newIORef ([] :: [B.ByteString])
            conn <- incomingConnection received (\bytes -> modifyIORef' output (bytes :))
            let app _ exchange = S.respond exchange status200 [] (\_ -> pure ())
            withAsync (withRequestBuffers resources $ \_ -> putMVar held () >> takeMVar releaseHeld) $ \holder -> do
                takeMVar held
                withAsync (serveHTTP1Connection (setTimeout 5 defaultSettings) resources conn TCP (SockAddrInet 0 0) app) $ \worker -> do
                    timeout 1000000 (takeMVar received) `shouldSatisfy` P.returns (P.eq (Just ()))
                    pending <- timeout 100000 (waitCatch worker)
                    case pending of
                        Nothing -> pure ()
                        Just result -> fail ("connection completed before cancellation: " ++ show result)
                    cancel worker
                    cancelled <- waitCatch worker
                    case cancelled of
                        Left exception -> case fromException exception :: Maybe AsyncCancelled of
                            Just _ -> pure ()
                            Nothing -> fail ("unexpected cancellation result: " ++ show exception)
                        Right () -> fail "connection returned normally after cancellation"
                    readIORef output `shouldSatisfy` P.returns (P.eq [])
                putMVar releaseHeld ()
                wait holder
                timeout 300000 (withRequestBuffers resources (\_ -> pure ())) `shouldSatisfy` P.returns (P.eq (Just ()))

    it "bounds a blocked 503 transport write" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            held <- newEmptyMVar
            releaseHeld <- newEmptyMVar
            received <- newEmptyMVar
            writing <- newEmptyMVar
            blocked <- newEmptyMVar
            conn <- incomingConnection received (\_ -> putMVar writing () >> takeMVar blocked)
            let app _ exchange = S.respond exchange status200 [] (\_ -> pure ())
            withAsync (withRequestBuffers resources $ \_ -> putMVar held () >> takeMVar releaseHeld) $ \holder -> do
                takeMVar held
                withAsync (serveHTTP1Connection (setTimeout 1 defaultSettings) resources conn TCP (SockAddrInet 0 0) app) $ \worker -> do
                    timeout 2000000 (takeMVar writing) `shouldSatisfy` P.returns (P.eq (Just ()))
                    completed <- timeout 3000000 (waitCatch worker)
                    case completed of
                        Nothing -> fail "blocked 503 write exceeded its timeout"
                        Just (Left exception) -> fail ("unexpected blocked-write result: " ++ show exception)
                        Just (Right ()) -> pure ()
                putMVar releaseHeld ()
                wait holder

    it "does not report admission overload for a stalled header read" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            withServerSettings (setTimeout 1 defaultSettings) resources
                (\_ exchange -> S.respond exchange status200 [] (\_ -> pure ())) $ \client -> do
                    NS.sendAll client "GET /slow HTTP/1.1\r\nHost: local\r\n"
                    response <- receiveAll client
                    B.isPrefixOf "HTTP/1.1 503" response `shouldSatisfy` P.eq False

    it "does not report admission overload for a stalled request body" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            called <- newEmptyMVar
            let app _ exchange = do
                    putMVar called ()
                    S.withBody exchange $ \source -> withChunk source (\_ -> pure ())
                    S.respond exchange status200 [] (\_ -> pure ())
            withServerSettings (setTimeout 1 defaultSettings) resources app $ \client -> do
                NS.sendAll client "POST /slow HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\n"
                timeout 300000 (takeMVar called) `shouldSatisfy` P.returns (P.eq (Just ()))
                response <- receiveAll client
                B.isPrefixOf "HTTP/1.1 503" response `shouldSatisfy` P.eq False

    it "admits body-free requests before dispatching their owned metadata" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            called <- newEmptyMVar
            let app request exchange = do
                    putMVar called ()
                    rawPathInfo (S.requestMetadata request) `shouldSatisfy` P.eq "/owned"
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            held <- newEmptyMVar
            release <- newEmptyMVar
            withAsync (withRequestBuffers resources $ \_ -> putMVar held () >> takeMVar release) $ \holder -> do
                takeMVar held
                withServer resources app $ \client -> do
                    NS.sendAll client "GET /owned HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                    timeout 30000 (takeMVar called) `shouldSatisfy` P.returns (P.eq Nothing)
                    putMVar release ()
                    wait holder
                    response <- receiveAll client
                    B.isSuffixOf "ok" response `shouldSatisfy` P.eq True

    it "keeps callback body bytes stable while a shared capability fills its second loan" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            ready <- newEmptyMVar
            let app request exchange = do
                    putMVar ready ()
                    S.withBody exchange $ \source -> withChunk source $ \chunk -> do
                        chunk `shouldSatisfy` P.eq "abc"
                        bracket (borrowBuffer (S.requestBuffers request)) releaseBuffer $ \loan -> do
                            let buffer = loanBuffer loan
                            withForeignPtr (bufferMemory buffer) $ \ptr -> fillBytes ptr 88 (bufferCapacity buffer)
                        chunk `shouldSatisfy` P.eq "abc"
                        S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink chunk)
            withServer resources app $ \client -> do
                NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\n"
                takeMVar ready
                NS.sendAll client "abc"
                response <- receiveAll client
                B.isSuffixOf "abc" response `shouldSatisfy` P.eq True

    it "returns a fully consumed input loan before the next body pull" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            ready <- newEmptyMVar
            let app request exchange = do
                    putMVar ready ()
                    S.withBody exchange $ \source -> do
                        withChunk source (\chunk -> chunk `shouldSatisfy` P.eq "abcdefgh")
                        -- The primary can be reused after the callback, so two
                        -- ordinary borrowers fit without requesting a third.
                        bracket (borrowBuffer (S.requestBuffers request)) releaseBuffer $ \_ ->
                            bracket (borrowBuffer (S.requestBuffers request)) releaseBuffer (const (pure ()))
                        withChunk source (\chunk -> chunk `shouldSatisfy` P.eq "ijk")
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            withServer resources app $ \client -> do
                NS.sendAll client "POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 11\r\nConnection: close\r\n\r\n"
                takeMVar ready
                NS.sendAll client "abcdefghijk"
                response <- receiveAll client
                B.isSuffixOf "ok" response `shouldSatisfy` P.eq True

    it "owns header metadata when every body region is overwritten after a read-ahead callback" $
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @128, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources -> do
            let app request exchange = do
                    S.withBody exchange $ \source -> do
                        withChunk source (\chunk -> chunk `shouldSatisfy` P.eq "abc")
                        let access = S.requestBuffers request
                            poison buffer = withForeignPtr (bufferMemory buffer) $ \ptr -> fillBytes ptr 88 (bufferCapacity buffer)
                        bracket (borrowBuffer access) releaseBuffer $ \first ->
                            bracket (borrowBuffer access) releaseBuffer $ \second -> do
                                poison (loanBuffer first)
                                poison (loanBuffer second)
                        rawPathInfo (S.requestMetadata request) `shouldSatisfy` P.eq "/owned"
                        lookup "Host" (requestHeaders (S.requestMetadata request)) `shouldSatisfy` P.eq (Just "local")
                    S.respond exchange status200 [("Content-Length", "2")] (\sink -> sink "ok")
            withServer resources app $ \client -> do
                NS.sendAll client "POST /owned HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"
                response <- receiveAll client
                B.isSuffixOf "ok" response `shouldSatisfy` P.eq True

getAcquisition :: IO Context
getAcquisition = newContext <$> getSystemTimerManager

withClient :: SockAddr -> (Socket -> IO a) -> IO a
withClient address action = bracket (socket AF_INET Stream defaultProtocol) close $ \client -> do
    connect client address
    action client

incomingConnection :: MVar () -> (B.ByteString -> IO ()) -> IO Connection
incomingConnection received send = do
    pending <- newIORef "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
    base <- connection send
    let receive ptr capacity = do
            bytes <- readIORef pending
            let (part, rest) = B.splitAt capacity bytes
            B.useAsCString part $ \src -> copyBytes ptr (castPtr src) (B.length part)
            writeIORef pending rest
            if B.null bytes then pure () else do
                empty <- isEmptyMVar received
                when empty $ putMVar received ()
            pure (B.length part)
    pure base { connRecvInto = Just receive }
