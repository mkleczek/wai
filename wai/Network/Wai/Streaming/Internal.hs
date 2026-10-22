{-# LANGUAGE RankNTypes #-}
-- | Server implementation hooks. Applications use Network.Wai.Streaming.
module Network.Wai.Streaming.Internal (Request (..), Exchange (..)) where

import Network.HTTP.Types (Status, ResponseHeaders)
import Network.Streaming (Source, Body)
import Network.Wai (Response)
import qualified Network.Wai as Wai
import Network.Streaming.BufferPool (BufferAccess)
import Network.Streaming.Buffer (Buffer)

-- | Metadata is owned independently of the request's body storage. The buffer
-- capability is shared with downstream consumers for this request only.
data Request = Request
    { requestMetadata :: !Wai.Request
    , requestBuffers :: !BufferAccess
    }

-- | A capability whose methods expire with their server invocation.
data Exchange = Exchange
    { exchangeWithBody :: forall a. (Source -> IO a) -> IO a
    , exchangeWithBodyBuffer :: forall a. Buffer -> (Source -> IO a) -> IO a
    , exchangeRespond :: Status -> ResponseHeaders -> Body -> IO ()
    , exchangeRespondResponse :: Response -> IO ()
    }
