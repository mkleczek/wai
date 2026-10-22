-- | Operation-local binding to Warp's monomorphic HTTP/1 body parsers.
module Network.Wai.Handler.Warp.Streaming.Input
    ( Receive, withRequestBodyBuffer ) where

import Data.ByteString (ByteString)
import Data.Word (Word8)
import Foreign.Ptr (Ptr)
import Network.Streaming (Source)
import Network.Streaming.Resources.Internal (Buffer)
import Network.Wai (RequestBodyLength)

type Receive = Ptr Word8 -> Int -> IO Int

-- | The second callback argument inspects parser read-ahead while the buffer
-- remains borrowed. The runner must consume it before ending the operation.
withRequestBodyBuffer :: RequestBodyLength -> ByteString -> Receive -> Buffer
    -> (Source -> IO ByteString -> IO a) -> IO a
withRequestBodyBuffer _ _ _ _ _ = ioError (userError "supplied body input is not implemented")
