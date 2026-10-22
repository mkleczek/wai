{-# LANGUAGE RankNTypes #-}
module Network.Wai.Handler.Warp.Streaming.Internal
    ( withHTTP1Exchange, withBodyBuffer, respondResponse ) where

import Network.Streaming (Source)
import Network.Streaming.BufferPool (BufferAccess)
import Network.Streaming.Buffer (Buffer)
import Network.Wai (Request, Response)
import Network.Wai.Streaming.Internal (Exchange (..))
import qualified Network.Wai.Streaming.Internal as Streaming

withBodyBuffer :: Exchange -> Buffer -> (Source -> IO a) -> IO a
withBodyBuffer = exchangeWithBodyBuffer

respondResponse :: Exchange -> Response -> IO ()
respondResponse = exchangeRespondResponse

withHTTP1Exchange :: BufferAccess -> Request
    -> (forall b. Maybe Buffer -> (Source -> IO b) -> IO b)
    -> (Response -> IO ())
    -> (Streaming.Request -> Exchange -> IO a) -> IO a
withHTTP1Exchange _ _ _ _ _ = ioError (userError "HTTP/1.1 exchange is not implemented")
