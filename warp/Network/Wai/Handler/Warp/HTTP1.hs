{-# LANGUAGE DataKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Wai.Handler.Warp.HTTP1 (
    http1, http1Streaming,
) where

import qualified Control.Concurrent as Conc (yield)
import qualified Control.Exception as E
import Network.Streaming.BufferPool (BufferPool, BufferAccess, withRequestBuffers)
import qualified Network.Streaming as Stream
import qualified Network.Wai.Streaming as Streaming
import Network.Wai.Handler.Warp.Streaming.Internal (withHTTP1Exchange)
import Network.Wai.Handler.Warp.Streaming.Input (Receive)
import Network.Wai.Handler.Warp.Streaming.InputContext
import Control.Exception (SomeException, catch, fromException, throwIO, try)
import qualified Data.ByteString as BS
import Data.Char (chr)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Word8 (_cr, _space)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (fillBytes)
import Network.Socket (SockAddr (SockAddrInet, SockAddrInet6))
import Network.Wai
import Network.Wai.Internal (ResponseReceived (ResponseReceived))
import qualified System.TimeManager as T
import System.Timeout (timeout)
import "iproute" Data.IP (toHostAddress, toHostAddress6)

import Network.Wai.Handler.Warp.Buffer (releaseWriteBuffer)
import Network.Wai.Handler.Warp.Header
import Network.Wai.Handler.Warp.Imports hiding (readInt)
import Network.Wai.Handler.Warp.ReadInt
import Network.Wai.Handler.Warp.Request
import Network.Wai.Handler.Warp.Response
import Network.Wai.Handler.Warp.Settings
import Network.Wai.Handler.Warp.Types

http1
    :: Settings
    -> InternalInfo
    -> Connection
    -> Transport
    -> Application
    -> SockAddr
    -> T.Handle
    -> ByteString
    -> IO ()
http1 = http1With Nothing

type StreamingMode = (BufferPool, Streaming.Application, IO ByteString, Receive)

http1Streaming :: BufferPool -> Settings -> InternalInfo -> Connection -> Transport
    -> Streaming.Application -> SockAddr -> T.Handle -> IO ()
http1Streaming resources settings ii conn transport app addr th = do
    receive <- maybe (ioError (userError "Warp transport cannot receive into supplied storage"))
        pure (connRecvInto conn)
    withMetadataReceive receive $ \metadata ->
        http1With (Just (resources, app, metadata, receive)) settings ii conn transport
            (\_ _ -> error "ordinary application unavailable in operation-stream runner") addr th BS.empty

http1With :: Maybe StreamingMode -> Settings -> InternalInfo -> Connection -> Transport
    -> Application -> SockAddr -> T.Handle -> ByteString -> IO ()
http1With streaming settings ii conn transport app origAddr th bs0 = do
    istatus <- newIORef True
    let wrapMetadata receive = wrappedRecv receive istatus (settingsSlowlorisSize settings)
        wrapBody receive ptr capacity = do
            count <- receive ptr capacity
            when (count >= settingsSlowlorisSize settings) $ T.tickle th
            pure count
        mode = fmap (\(resources, handler, metadata, receive) ->
            (resources, handler, wrapMetadata metadata, wrapBody receive)) streaming
        selectedReceive = case mode of
            Nothing -> wrapMetadata (connRecv conn)
            Just (_, _, metadata, _) -> metadata
    src <- mkSource selectedReceive
    leftoverSource src bs0
    addr <- getProxyProtocolAddr src
    http1server mode settings ii conn transport app addr th istatus src
  where
    wrappedRecv recv istatus slowlorisSize = do
        bs <- recv
        unless (BS.null bs) $ do
            writeIORef istatus True
            when (BS.length bs >= slowlorisSize) $ T.tickle th
        return bs

    getProxyProtocolAddr src =
        case settingsProxyProtocol settings of
            ProxyProtocolNone ->
                return origAddr
            ProxyProtocolRequired -> do
                seg <- proxySegment True src
                parseProxyProtocolHeader src seg
            ProxyProtocolOptional -> do
                seg <- proxySegment False src
                if BS.isPrefixOf "PROXY " seg
                    then parseProxyProtocolHeader src seg
                    else do
                        leftoverSource src seg
                        return origAddr

    -- Streaming readiness reads one byte without a body grant. PROXY is
    -- bounded control metadata and must be assembled before HTTP admission.
    proxySegment required src = case streaming of
        Nothing -> readSource src
        Just _ -> gather BS.empty
      where
        gather bytes
            | BS.elem 10 bytes = pure bytes
            | not required && BS.length bytes >= 6 && not (BS.isPrefixOf "PROXY " bytes) = pure bytes
            | BS.length bytes >= 108 = throwIO (BadProxyHeader (decodeAscii bytes))
            | otherwise = do
                next <- readSource src
                if BS.null next then pure bytes else gather (bytes <> next)

    parseProxyProtocolHeader src seg = do
        let (header, seg') = BS.break (== _cr) seg
            maybeAddr = case BS.split _space header of
                ["PROXY", "TCP4", clientAddr, _, clientPort, _] ->
                    case [x | (x, t) <- reads (decodeAscii clientAddr), null t] of
                        [a] ->
                            Just
                                ( SockAddrInet
                                    (readInt clientPort)
                                    (toHostAddress a)
                                )
                        _ -> Nothing
                ["PROXY", "TCP6", clientAddr, _, clientPort, _] ->
                    case [x | (x, t) <- reads (decodeAscii clientAddr), null t] of
                        [a] ->
                            Just
                                ( SockAddrInet6
                                    (readInt clientPort)
                                    0
                                    (toHostAddress6 a)
                                    0
                                )
                        _ -> Nothing
                ("PROXY" : "UNKNOWN" : _) ->
                    Just origAddr
                _ ->
                    Nothing
        case maybeAddr of
            Nothing -> throwIO (BadProxyHeader (decodeAscii header))
            Just a -> do
                leftoverSource src (BS.drop 2 seg') -- drop CRLF
                return a

    decodeAscii = map (chr . fromEnum) . BS.unpack

http1server :: Maybe StreamingMode -> Settings -> InternalInfo -> Connection
    -> Transport -> Application -> SockAddr -> T.Handle -> IORef Bool -> Source -> IO ()
http1server streaming settings ii conn transport app addr th istatus initialSource =
    loop FirstRequest initialSource `catch` handler
  where
    handler e
        -- See comment below referencing
        -- https://github.com/yesodweb/wai/issues/618
        | Just NoKeepAliveRequest <- fromException e = return ()
        -- No valid request
        | Just (BadFirstLine _) <- fromException e = return ()
        | isAsyncException e = throwIO e
        | otherwise = do
            _ <-
                sendErrorResponse
                    settings
                    ii
                    conn
                    th
                    istatus
                    defaultRequest{remoteHost = addr}
                    e
            throwIO e

    loop firstRequest previous = do
        (keepAlive, next) <- case streaming of
            Nothing -> do
                decision <- serve firstRequest previous Nothing
                pure (decision, previous)
            Just (resources, appHandler, metadata, receive) -> do
                leftover <- readLeftoverSource previous
                prefix <- if BS.null leftover then metadata else pure leftover
                when (BS.null prefix) $ case firstRequest of
                    FirstRequest -> throwIO ConnectionClosedByPeer
                    SubsequentRequest -> throwIO NoKeepAliveRequest
                writeIORef istatus True
                E.mask $ \restore -> do
                    waiting <- newIORef True
                    -- Primary admission uses the connection's request timeout.
                    T.pause th
                    T.resume th
                    restore (withRequestBuffers resources $ \access -> do
                        writeIORef waiting False
                        T.resume th
                        input <- newInputContext access receive prefix
                        let src = contextSource input
                        flip E.finally (closeInputContext input) $ do
                            decision <- serve firstRequest src (Just (access, appHandler, input))
                            prefix' <- inputLeftover input
                            next <- mkSource metadata
                            leftoverSource next prefix'
                            pure (decision, next)) `E.catch` \T.TimeoutThread -> do
                        stillWaiting <- readIORef waiting
                        if stillWaiting
                            then do
                                -- The acquisition scope has released any late grant.
                                -- Give this direct write its own timeout.
                                writeIORef istatus False
                                T.pause th
                                T.resume th
                                sent <- E.try (restore (connSendAll conn admissionUnavailable))
                                case sent of
                                    Left (_ :: E.IOException) -> pure ()
                                    Right () -> do
                                        T.pause th
                                        void $ restore $ timeout 250000 (drainRejectedRequest receive)
                                pure (CloseConnection, previous)
                            else throwIO T.TimeoutThread
        case keepAlive of
            ReuseConnection -> loop SubsequentRequest next
            CloseConnection -> return ()

    serve firstRequest src operation = do
        (req, mremainingRef, idxhdr, nextBodyFlush) <-
            recvRequest firstRequest settings conn ii th addr src transport
        case operation of
            Just (_, _, input) -> headersComplete input
            Nothing -> pure ()
        processRequest operation
                settings
                ii
                conn
                app
                th
                istatus
                src
                req
                mremainingRef
                idxhdr
                nextBodyFlush
                `catch` \e -> do
                    settingsOnException settings (Just req) e
                    -- Don't throw the error again to prevent calling settingsOnException twice.
                    return CloseConnection

data ReuseConnection = ReuseConnection | CloseConnection

admissionUnavailable :: ByteString
admissionUnavailable =
    "HTTP/1.1 503 Service Unavailable\r\nWarp-Error: request-buffer-timeout\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"

-- An unread upload can turn an immediate TCP close into a reset that hides
-- the 503. Consume a small amount of already arriving data before closing.
-- The caller also bounds this entire drain by wall-clock time.
drainRejectedRequest :: Receive -> IO ()
drainRejectedRequest receive = allocaBytes 1024 $ \scratch -> do
    fillBytes scratch 0 1024
    loop 16384 scratch
  where
    loop remaining scratch
        | remaining <= 0 = pure ()
        | otherwise = do
            count <- receive scratch (min 1024 remaining) `E.catch` \(_ :: E.IOException) -> pure 0
            when (count > 0 && count <= min 1024 remaining) $ loop (remaining - count) scratch

processRequest :: Maybe (BufferAccess, Streaming.Application, InputContext)
    -> Settings -> InternalInfo -> Connection -> Application -> T.Handle -> IORef Bool
    -> Source -> Request -> Maybe (IORef Int) -> IndexedRequestHeader -> IO ByteString
    -> IO ReuseConnection
processRequest operation settings ii conn app th istatus src req mremainingRef idxhdr nextBodyFlush = do
    -- Let the application run for as long as it wants
    T.pause th

    -- In the event that some scarce resource was acquired during
    -- creating the request, we need to make sure that we don't get
    -- an async exception before calling the ResponseSource.
    keepAliveRef <- newIORef False
    finishedRef <- newIORef Nothing
    let releaseOutput = case operation of
            Nothing -> pure ()
            Just _ -> releaseWriteBuffer (connWriteBuffer conn)
        finish = finishRequestBody settings th mremainingRef nextBodyFlush
        attach input buffer use = withInputBuffer input buffer $ do
            result <- use (Stream.Source $ \consume -> E.mask $ \restore -> do
                chunk <- restore (getRequestBodyChunk req)
                restore (consume chunk) `E.finally` releaseConsumedInput input)
            startedResponse <- not <$> readIORef istatus
            persistent <- readIORef keepAliveRef
            remaining <- traverse readIORef mremainingRef
            -- Before a final response, an unpulled expected upload needs its
            -- 100 Continue before this scope can drain and return the buffer.
            let drain = if startedResponse || remaining == Just 0
                    then nextBodyFlush else getRequestBodyChunk req
            decision <- finishRequestBody settings th mremainingRef drain
                (not startedResponse || persistent)
            writeIORef finishedRef (Just decision)
            T.pause th
            pure result
        runApplication respond = case operation of
            Nothing -> app req respond
            Just (access, handler, input) -> do
                withHTTP1Exchange access req
                    (\buffer use -> attach input buffer use
                        `E.onException` writeIORef finishedRef (Just CloseConnection))
                    (void . respond) handler
                pure ResponseReceived
    r <- try $ runApplication $ \res -> do
        T.resume th
        -- FIXME consider forcing evaluation of the res here to
        -- send more meaningful error messages to the user.
        -- However, it may affect performance.
        writeIORef istatus False
        keepAlive <- sendResponse settings conn ii th req idxhdr (readSource src) res
            `E.finally` releaseOutput
        writeIORef keepAliveRef keepAlive
        return ResponseReceived
    case r of
        Right ResponseReceived -> return ()
        Left (e :: SomeException)
            | Just (ExceptionInsideResponseBody e') <- fromException e -> throwIO e'
            | isAsyncException e -> throwIO e
            | otherwise -> do
                keepAlive <- sendErrorResponse settings ii conn th istatus req e
                    `E.finally` releaseOutput
                settingsOnException settings (Just req) e
                writeIORef keepAliveRef (case operation of Nothing -> keepAlive; Just _ -> False)

    keepAlive <- readIORef keepAliveRef

    -- We just send a Response and it takes a time to
    -- receive a Request again. If we immediately call recv,
    -- it is likely to fail and cause the IO manager to do some work.
    -- It is very costly, so we yield to another Haskell
    -- thread hoping that the next Request will arrive
    -- when this Haskell thread will be re-scheduled.
    -- This improves performance at least when
    -- the number of cores is small.
    Conc.yield

    if not keepAlive then pure CloseConnection else do
        finished <- readIORef finishedRef
        case finished of
            Just decision -> T.resume th >> pure decision
            Nothing -> case operation of
                Nothing -> finish True
                Just (_, _, input) -> do
                    remaining <- traverse readIORef mremainingRef
                    case remaining of
                        Just 0 -> finish True
                        Just count | Just limit <- settingsMaximumBodyFlush settings
                            , count > limit -> pure CloseConnection
                        _ -> withInputBuffer input Nothing (finish True)

finishRequestBody :: Settings -> T.Handle -> Maybe (IORef Int) -> IO ByteString
    -> Bool -> IO ReuseConnection
finishRequestBody settings th mremainingRef nextBodyFlush keepAlive =
    if keepAlive
        then -- If there is an unknown or large amount of data to still be read
        -- from the request body, simple drop this connection instead of
        -- reading it all in to satisfy a keep-alive request.
            case settingsMaximumBodyFlush settings of
                Nothing -> do
                    flushEntireBody nextBodyFlush
                    T.resume th
                    return ReuseConnection
                Just maxToRead -> do
                    let tryKeepAlive = do
                            -- flush the rest of the request body
                            isComplete <- flushBody nextBodyFlush maxToRead
                            if isComplete
                                then do
                                    T.resume th
                                    return ReuseConnection
                                else return CloseConnection
                    case mremainingRef of
                        Just ref -> do
                            remaining <- readIORef ref
                            if remaining <= maxToRead
                                then tryKeepAlive
                                else return CloseConnection
                        Nothing -> tryKeepAlive
        else return CloseConnection

sendErrorResponse
    :: Settings
    -> InternalInfo
    -> Connection
    -> T.Handle
    -> IORef Bool
    -> Request
    -> SomeException
    -> IO Bool
sendErrorResponse settings ii conn th istatus req e = do
    status <- readIORef istatus
    if shouldSendErrorResponse e && status
        then
            sendResponse
                settings
                conn
                ii
                th
                req
                defaultIndexRequestHeader
                (return BS.empty)
                errorResponse
        else return False
  where
    shouldSendErrorResponse se
        | Just ConnectionClosedByPeer <- fromException se = False
        | otherwise = True
    errorResponse = settingsOnExceptionResponse settings e

flushEntireBody :: IO ByteString -> IO ()
flushEntireBody src =
    loop
  where
    loop = do
        bs <- src
        unless (BS.null bs) loop

flushBody
    :: IO ByteString
    -- ^ get next chunk
    -> Int
    -- ^ maximum to flush
    -> IO Bool
    -- ^ True == flushed the entire body, False == we didn't
flushBody src = loop
  where
    loop toRead = do
        bs <- src
        let toRead' = toRead - BS.length bs
        case () of
            ()
                | BS.null bs -> return True
                | toRead' >= 0 -> loop toRead'
                | otherwise -> return False
