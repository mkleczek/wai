-- | Operation-local binding to Warp's monomorphic HTTP/1 body parsers.
module Network.Wai.Handler.Warp.Streaming.Input
    ( Receive, withRequestBodyBuffer ) where

import qualified Control.Exception as E
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Data.IORef
import Data.Word (Word8)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (Ptr)
import Network.Streaming (Source (..))
import Network.Streaming.Buffer (Buffer (..))
import Network.Wai (RequestBodyLength (..))
import Network.Wai.Handler.Warp.Conduit
import qualified Network.Wai.Handler.Warp.Types as Warp
import Network.Wai.Handler.Warp.Streaming.Gate

type Receive = Ptr Word8 -> Int -> IO Int

-- | The second callback argument inspects parser read-ahead while the buffer
-- remains borrowed. The runner must consume it before ending the operation.
-- No connection cursor or receive callback is replaced during this binding.
withRequestBodyBuffer :: RequestBodyLength -> ByteString -> Receive -> Buffer
    -> (Source -> IO ByteString -> IO a) -> IO a
withRequestBodyBuffer lengthHint prefix receive buffer use = do
    unless (bufferCapacity buffer > 0) unavailable
    budget <- case lengthHint of
        KnownLength size -> do
            when (toInteger size > toInteger (maxBound :: Int)) unavailable
            pure $ Just (fromIntegral size)
        ChunkedBody -> pure Nothing
    remaining <- newIORef (fmap (\size -> max 0 (size - B.length prefix)) budget)
    let refill = do
            left <- readIORef remaining
            let capacity = maybe (bufferCapacity buffer) (min (bufferCapacity buffer)) left
            if capacity == 0 then pure B.empty else
                withForeignPtr (bufferMemory buffer) $ \ptr -> do
                    count <- receive ptr capacity
                    unless (count >= 0 && count <= capacity) unavailable
                    modifyIORef' remaining (fmap (subtract count))
                    pure (BI.fromForeignPtr (bufferMemory buffer) 0 count)
    raw <- Warp.mkSource refill
    Warp.leftoverSource raw prefix
    source <- case budget of
        Just size -> readISource <$> mkISource raw size
        Nothing -> readCSource <$> mkCSource raw
    gate <- newOperationGate
    let close = closeOperationGate gate >>= \busy -> when busy unavailable
    use (Source $ \consume -> runOperation gate (source >>= consume)) (runOperation gate (Warp.readLeftoverSource raw))
        `E.finally` close
