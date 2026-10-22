-- | HTTP/1.1 operation-stream server. HTTP/2 and upgrades are unavailable.
module Network.Wai.Handler.Warp.Streaming (runHTTP1, runHTTP1Socket) where

import Control.Exception (bracket)
import Data.Streaming.Network (bindPortTCP)
import Network.Socket (Socket, close, withSocketsDo)
import Network.Streaming.BufferPool (BufferPool)
import qualified Network.Wai.Streaming as Streaming
import Network.Wai.Handler.Warp.Settings (Settings (..))
import qualified Network.Wai.Handler.Warp.Run as Run

runHTTP1 :: Settings -> BufferPool -> Streaming.Application -> IO ()
runHTTP1 settings resources application = withSocketsDo $
    bracket (bindPortTCP (settingsPort settings) (settingsHost settings)) close $ \socket -> do
        Run.setSocketCloseOnExec socket
        runHTTP1Socket settings resources socket application

runHTTP1Socket :: Settings -> BufferPool -> Socket -> Streaming.Application -> IO ()
runHTTP1Socket settings resources socket application =
    Run.runSettingsSocketWith (Just (resources, application)) settings socket
        (\_ _ -> error "ordinary application unavailable in HTTP/1 operation runner")
