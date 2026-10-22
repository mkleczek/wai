{-# LANGUAGE DataKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module TransportSpec (spec) where

import Control.Concurrent.Resource.Acquisition (Context, newContext)
import GHC.Event (getSystemTimerManager)
import Control.Concurrent (newMVar, readMVar)
import Control.Monad (when)
import Control.Exception (evaluate, finally)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Data.IORef
import Foreign.ForeignPtr (withForeignPtr, mallocForeignPtrBytes)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (castPtr, plusPtr)
import Network.HTTP.Types (status200)
import Network.Socket (SockAddr (..))
import Network.Streaming (Source (..))
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Streaming.Buffer (Buffer (..))
import Network.Wai.Handler.Warp.Streaming.Internal (withBodyBuffer, respondResponse)
import Network.Wai (isSecure, responseLBS)
import qualified Network.Wai.Streaming as S
import Network.Wai.Handler.Warp (defaultSettings, setOnException)
import Network.Wai.Handler.Warp.Internal (Connection (..), Transport (..), WriteBuffer (..), serveHTTP1Connection)
import OutputSpec (connection)
import Skeletest
import qualified Skeletest.Predicate as P

spec :: Spec
spec = describe "blocking transport callback ownership" $ do
    it "bulk-reads headers, drives TLS write/read progress itself, and consumes borrowed plaintext synchronously" $ do
        packets <- newIORef ["POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\nConnection: close\r\n\r\n", "abc"]
        capacities <- newIORef []
        events <- newIORef ([] :: [String])
        output <- newIORef []
        bodyPointer <- newIORef Nothing
        writable <- newMVar ()
        readable <- newMVar ()
        base <- connection $ \bytes -> do
            owned <- evaluate (B.copy bytes)
            modifyIORef' output (owned :)
        let receive ptr capacity = do
                modifyIORef' capacities (capacity :)
                chunks <- readIORef packets
                case chunks of
                    [] -> pure 0
                    chunk : rest -> do
                        when (chunk == "abc") $ do
                            -- A backend can require writable readiness and send
                            -- encrypted control bytes before plaintext reads.
                            readMVar writable
                            modifyIORef' events ("TLS write" :)
                            readMVar readable
                            modifyIORef' events ("TLS read" :)
                            writeIORef bodyPointer (Just ptr)
                        let (part, extra) = B.splitAt capacity chunk
                        B.useAsCString part $ \src -> copyBytes ptr (castPtr src) (B.length part)
                        writeIORef packets (if B.null extra then rest else extra : rest)
                        pure (B.length part)
            transport = base
                { connRecv = failTest "runner used legacy allocated receiving"
                , connRecvInto = Just receive
                }
        getAcquisition >>= \acquisition -> newBufferPool acquisition (PoolSettings { bufferSize = positiveInt @8, ordinaryBufferCount = positiveInt @1, reserveBufferCount = positiveInt @1, setupBufferCount = positiveInt @1 }) >>= \resources ->
            serveHTTP1Connection defaultSettings resources transport (TLS 3 3 Nothing 0
#ifdef MIN_VERSION_crypton_x509
                    Nothing
#endif
                )
                (SockAddrInet 0 0) $ \request exchange -> do
                    isSecure (S.requestMetadata request) `shouldSatisfy` P.eq True
                    S.withBody exchange $ \source -> withChunk source $ \chunk -> do
                        Just expected <- readIORef bodyPointer
                        let (fp, offset, _) = BI.toForeignPtr chunk
                        withForeignPtr fp $ \ptr -> (ptr `plusPtr` offset) `shouldSatisfy` P.eq expected
                        S.respond exchange status200 [("Content-Length", "3")] (\sink -> sink chunk)
                        fillBytes expected 90 3
        observedCapacities <- reverse <$> readIORef capacities
        head observedCapacities `shouldSatisfy` P.eq 1
        all (== 8) (tail observedCapacities) `shouldSatisfy` P.eq True
        reverse <$> readIORef events `shouldSatisfy` P.returns (P.eq ["TLS write", "TLS read"])
        result <- B.concat . reverse <$> readIORef output
        B.isSuffixOf "abc" result `shouldSatisfy` P.eq True


getAcquisition :: IO Context
getAcquisition = newContext <$> getSystemTimerManager
