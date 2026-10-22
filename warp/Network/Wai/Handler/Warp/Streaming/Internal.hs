{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Network.Wai.Handler.Warp.Streaming.Internal
    ( withHTTP1Exchange, withBodyBuffer, respondResponse ) where

import Control.Concurrent (myThreadId)
import qualified Control.Exception as E
import Control.Monad (unless, when)
import Data.IORef
import qualified Network.HTTP.Types as H
import Network.Streaming (Source (..))
import Network.Streaming.BufferPool (BufferAccess)
import Network.Streaming.Buffer (Buffer (..))
import Network.Wai (Request, Response, httpVersion, requestHeaders, setRequestBodyChunks)
import Network.Wai.Internal (Response (..))
import Network.Wai.Streaming.Internal (Exchange (..))
import qualified Network.Wai.Streaming.Internal as Streaming
import Network.Wai.Handler.Warp.Streaming.Gate

withBodyBuffer :: Exchange -> Buffer -> (Source -> IO a) -> IO a
withBodyBuffer = exchangeWithBodyBuffer

respondResponse :: Exchange -> Response -> IO ()
respondResponse = exchangeRespondResponse

data Mode = Available | Borrowing | Returned | Expired deriving (Eq)

-- | Attach to Warp's existing HTTP/1 body parser and synchronous responder.
-- The backend owns framing and operation-local parser cursors. It must finish
-- all parser use and borrowed callbacks before returning from attach. The
-- optional descriptor exists only for low-level supplied-buffer integration.
withHTTP1Exchange :: BufferAccess -> Request
    -> (forall b. Maybe Buffer -> (Source -> IO b) -> IO b)
    -> (Response -> IO ())
    -> (Streaming.Request -> Exchange -> IO a) -> IO a
withHTTP1Exchange buffers request attach send application = do
    unless (httpVersion request == H.http11) unavailable
    when (any ((== "Upgrade") . fst) (requestHeaders request)) unavailable
    owner <- myThreadId
    state <- newIORef (Available, False)
    reader <- newOperationGate
    let requireOwner = myThreadId >>= \tid -> unless (tid == owner) unavailable
        select predicate update = do
            allowed <- atomicModifyIORef' state $ \old ->
                if predicate old then (update old, True) else (old, False)
            unless allowed unavailable
        expire mode = do
            atomicModifyIORef' state $ \(_, sent) -> ((mode, sent), ())
            busy <- closeOperationGate reader
            when busy unavailable
        selected provided use = E.mask $ \restore -> do
            requireOwner
            mapM_ (\buffer -> unless (bufferCapacity buffer > 0) unavailable) provided
            select ((== Available) . fst) (\(_, sent) -> (Borrowing, sent))
            attach provided $ \source ->
                restore (use $ Source $ \consume -> runOperation reader $ do
                    (mode, _) <- readIORef state
                    unless (mode == Borrowing) unavailable
                    withChunk source consume) `E.finally` expire Returned
        supplied buffer = selected (Just buffer)
        pooled = selected Nothing
        scoped use = E.mask $ \restore -> do
            gate <- newOperationGate
            let finish = closeOperationGate gate >>= \busy -> when busy unavailable
            restore (use (runOperation gate)) `E.finally` finish
        protect response = case response of
            ResponseBytes status headers body -> ResponseBytes status headers $ \sink ->
                scoped $ \run -> body (run . sink)
            ResponseStream status headers body -> ResponseStream status headers $ \sink flush ->
                scoped $ \run -> body (run . sink) (run flush)
            _ -> response
        respondExisting response = do
            requireOwner
            case response of
                ResponseRaw{} -> unavailable
                _ -> pure ()
            select (\(mode, sent) -> mode /= Expired && not sent)
                (\(mode, _) -> (mode, True))
            send (protect response)
        respond status headers body = respondExisting (ResponseBytes status headers body)
        exchange = Exchange pooled supplied respond respondExisting
        metadata = Streaming.Request (setRequestBodyChunks unavailable request) buffers
    application metadata exchange `E.finally` expire Expired
