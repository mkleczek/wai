-- | HTTP operation streams. Chunks are borrowed only during their callback.
-- A sink consumes the entire chunk before returning; copy before retaining chunks or handing them to asynchronous consumers.
module Network.Wai.Streaming
    ( Application, Request, Exchange, Source, Sink, Body, forward
    , requestMetadata, requestBuffers, withBody, respond
    ) where

import Network.HTTP.Types (Status, ResponseHeaders)
import Network.Streaming (Source, Sink, Body, forward)
import Network.Wai.Streaming.Internal (Request (..), Exchange (..))

-- | requestMetadata provides the ordinary WAI metadata. Its owned body reader is
-- unavailable: use withBody to select the borrowed body for this operation.
type Application = Request -> Exchange -> IO ()

-- | Borrow the request body once. Pulls are exclusive and expire at scope exit.
withBody :: Exchange -> (Source -> IO a) -> IO a
withBody = exchangeWithBody

-- | Send one response synchronously. Its sink expires when the body returns.
respond :: Exchange -> Status -> ResponseHeaders -> Body -> IO ()
respond = exchangeRespond
