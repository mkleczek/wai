-- | HTTP/1.1 operation-stream server. HTTP/2 and upgrades are unavailable.
module Network.Wai.Handler.Warp.Streaming (runHTTP1, runHTTP1Socket) where

import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool)
import qualified Network.Wai.Streaming as Streaming
import Network.Wai.Handler.Warp.Settings (Settings)

runHTTP1 :: Settings -> BufferPool -> Streaming.Application -> IO ()
runHTTP1 _ _ _ = ioError (userError "HTTP/1 runner is not implemented")

runHTTP1Socket :: Settings -> BufferPool -> Socket -> Streaming.Application -> IO ()
runHTTP1Socket _ _ _ _ = ioError (userError "HTTP/1 runner is not implemented")
