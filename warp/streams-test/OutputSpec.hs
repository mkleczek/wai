{-# LANGUAGE OverloadedStrings #-}
module OutputSpec (spec) where

import Control.Exception (SomeException, try, evaluate)
import Control.Concurrent.STM (newTVarIO)
import Data.Either (isLeft)
import Data.IORef
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Foreign.ForeignPtr (mallocForeignPtrBytes, withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Foreign.Ptr (nullPtr)
import Network.HTTP.Types (status200, http11)
import Network.Socket (SockAddr (..))
import Network.Wai (defaultRequest, httpVersion)
import Network.Wai.Internal (Response (..))
import Network.Wai.Handler.Warp (defaultSettings)
import Network.Wai.Handler.Warp.Internal
import Skeletest
import qualified Skeletest.Predicate as P

-- All sends copy immediately, as a blocking transport must. The unused Builder
-- scratch is deliberately empty: borrowed body bytes must never enter it.
connection :: (B.ByteString -> IO ()) -> IO Connection
connection send = do
    buffer <- newIORef (WriteBuffer nullPtr 0 (pure ()))
    h2 <- newIORef False
    apps <- newTVarIO 0
    pure Connection
        { connSendAll = send, connSendMany = mapM_ send
        , connSendFile = \_ _ _ _ _ -> pure (), connClose = pure ()
        , connRecv = pure "", connRecvBuf = \_ _ -> pure False
        , connWriteBuffer = buffer, connHTTP2 = h2
        , connMySockAddr = SockAddrInet 0 0, connAppsInProgress = apps
        }

send :: Connection -> Response -> IO ()
send conn response = withII defaultSettings $ \info -> do
    _ <- sendResponse defaultSettings conn info emptyHandle
        (defaultRequest { httpVersion = http11 }) defaultIndexRequestHeader
        (pure "") response
    pure ()

spec :: Spec
spec = describe "synchronous direct-byte HTTP response" $ do
    it "consumes borrowed slices before returning and bypasses Builder scratch" $ do
        fp <- mallocForeignPtrBytes 3
        output <- newIORef []
        direct <- newIORef False
        conn <- connection $ \chunk -> do
            let (actual, _, _) = BI.toForeignPtr chunk
            if actual == fp then writeIORef direct True else pure ()
            owned <- evaluate (B.copy chunk)
            modifyIORef' output (owned :)
        let fill byte = withForeignPtr fp $ \ptr -> fillBytes ptr byte 3
            bytes = BI.fromForeignPtr fp 0 3
        send conn $ ResponseBytes status200 [("Content-Length", "6")] $ \sink -> do
            fill 65
            sink bytes
            fill 66
            sink bytes
            fill 67
        result <- B.concat . reverse <$> readIORef output
        B.isSuffixOf "AAABBB" result `shouldSatisfy` P.eq True
        readIORef direct `shouldSatisfy` P.returns (P.eq True)
    it "frames each chunk and expires retained sinks" $ do
        output <- newIORef []
        conn <- connection $ \chunk -> evaluate (B.copy chunk) >>= \owned -> modifyIORef' output (owned :)
        retained <- newIORef (\_ -> pure ())
        send conn $ ResponseBytes status200 [] $ \sink -> do
            writeIORef retained sink
            sink "abc"
            sink ""
        sink <- readIORef retained
        outcome <- try (sink "late") :: IO (Either SomeException ())
        isLeft outcome `shouldSatisfy` P.eq True
        result <- B.concat . reverse <$> readIORef output
        B.isSuffixOf "3\r\nabc\r\n0\r\n\r\n" result `shouldSatisfy` P.eq True
    it "rejects too few and too many content-length bytes" $ do
        conn <- connection (\_ -> pure ())
        let check body = do
                result <- try (send conn (ResponseBytes status200 [("Content-Length", "3")] body))
                    :: IO (Either SomeException ())
                isLeft result `shouldSatisfy` P.eq True
        check (\sink -> sink "ab")
        check (\sink -> sink "abcd")
        send conn (ResponseBytes status200 [("Content-Length", "3")] (\sink -> sink "abc"))
