{-# LANGUAGE OverloadedStrings #-}
module InputSpec (spec) where

import Control.Exception (SomeException, try, evaluate)
import Data.Either (isLeft)
import Data.IORef
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Foreign.ForeignPtr (mallocForeignPtrBytes)
import Foreign.Ptr (castPtr)
import Foreign.Marshal.Utils (copyBytes)
import Network.Streaming (Source (..))
import Network.Streaming.Buffer (Buffer (..))
import Network.Wai (RequestBodyLength (..))
import Network.Wai.Handler.Warp.Streaming.Input
import Skeletest
import qualified Skeletest.Predicate as P

script :: [B.ByteString] -> IO Receive
script chunks = do
    pending <- newIORef chunks
    pure $ \ptr capacity -> do
        remaining <- readIORef pending
        case remaining of
            [] -> pure 0
            chunk : rest -> do
                let (part, extra) = B.splitAt capacity chunk
                B.useAsCString part $ \src -> copyBytes ptr (castPtr src) (B.length part)
                writeIORef pending (if B.null extra then rest else extra : rest)
                pure (B.length part)

pull :: Source -> IO B.ByteString
pull source = withChunk source (evaluate . B.copy)

spec :: Spec
spec = describe "operation-local supplied HTTP body parser" $ do
    it "uses supplied storage and limits known-length receives before pipelined bytes" $ do
        fp <- mallocForeignPtrBytes 8
        receive <- script ["abcNEXT"]
        withRequestBodyBuffer (KnownLength 3) "" receive (Buffer fp 8) $ \source leftovers -> do
            withChunk source $ \chunk -> do
                let (actual, _, _) = BI.toForeignPtr chunk
                actual `shouldSatisfy` P.eq fp
                chunk `shouldSatisfy` P.eq "abc"
            pull source `shouldSatisfy` P.returns (P.eq "")
            leftovers `shouldSatisfy` P.returns (P.eq "")
        withRequestBodyBuffer (KnownLength 4) "" receive (Buffer fp 8) $ \source _ ->
            pull source `shouldSatisfy` P.returns (P.eq "NEXT")
    it "reuses the chunk decoder across fragmented framing" $ do
        fp <- mallocForeignPtrBytes 8
        receive <- script ["3\r", "\nabc\r", "\n0\r\n\r\nNEXT"]
        withRequestBodyBuffer ChunkedBody "" receive (Buffer fp 8) $ \source leftovers -> do
            pull source `shouldSatisfy` P.returns (P.eq "abc")
            pull source `shouldSatisfy` P.returns (P.eq "")
            leftovers `shouldSatisfy` P.returns (P.eq "NE")
    it "rejects invalid receive counts before constructing a borrowed slice" $ do
        fp <- mallocForeignPtrBytes 8
        result <- try (withRequestBodyBuffer (KnownLength 3) "" (\_ _ -> pure 9)
            (Buffer fp 8) (\source _ -> pull source)) :: IO (Either SomeException B.ByteString)
        isLeft result `shouldSatisfy` P.eq True
        withRequestBodyBuffer (KnownLength 0) "" (\_ _ -> pure 0)
            (Buffer fp 8) (\source _ -> pull source `shouldSatisfy` P.returns (P.eq ""))
