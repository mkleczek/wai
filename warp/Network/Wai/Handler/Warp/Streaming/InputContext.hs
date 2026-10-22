{-# LANGUAGE TupleSections #-}
-- | Per-request receive binding. Connections retain transport state only.
module Network.Wai.Handler.Warp.Streaming.InputContext
    ( InputContext, newInputContext, contextSource, headersComplete
    , withInputBuffer, releaseConsumedInput, closeInputContext, inputLeftover
    , withMetadataReceive, metadataBufferBytes
    ) where

import qualified Control.Exception as E
import Control.Monad (unless, when)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BI
import Data.IORef
import Foreign.ForeignPtr (mallocForeignPtrBytes, withForeignPtr)
import Foreign.Ptr (castPtr)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.BufferPool
    ( BufferAccess, BufferLoan, borrowBuffer, loanBuffer, releaseBuffer )
import Network.Wai.Handler.Warp.Streaming.Input (Receive)
import Network.Wai.Handler.Warp.Types

-- Only a bounded pipeline prefix can survive request teardown. Metadata itself
-- is separately owned, and constrained by Warp's total-header limit.
metadataBufferBytes :: Int
metadataBufferBytes = 16384

-- Read only enough to establish request activity before primary admission.
-- An idle connection therefore owns no request buffer.
withMetadataReceive :: Receive -> (IO B.ByteString -> IO a) -> IO a
withMetadataReceive receive use = do
    memory <- mallocForeignPtrBytes 1
    use $ withForeignPtr memory $ \ptr -> do
        count <- receive ptr 1
        validateCount 1 count
        B.packCStringLen (castPtr ptr, count)

data Binding = Metadata | Idle | Body (Maybe Buffer)

data InputContext = InputContext
    { contextSource :: !Source
    , contextBinding :: !(IORef Binding)
    , contextLoan :: !(IORef (Maybe BufferLoan))
    , contextPrefix :: !(IORef B.ByteString)
    }

-- Header receives use the request primary too, but copy only header bytes to
-- GC memory before another receive can overwrite them. Any body read-ahead
-- keeps its loan and is delivered directly when the application pulls it.
newInputContext :: BufferAccess -> Receive -> B.ByteString -> IO InputContext
newInputContext access receive prefix = do
    unless (B.length prefix <= metadataBufferBytes) invalid
    binding <- newIORef Metadata
    loan <- newIORef Nothing
    pending <- newIORef prefix
    lineHasContent <- newIORef False
    let release = atomicModifyIORef' loan (Nothing,) >>= mapM_ releaseBuffer
        receiveLoan = E.mask_ $ do
            release
            current <- borrowBuffer access
            writeIORef loan (Just current)
            receiveBuffer (loanBuffer current)
        receiveBuffer buffer = withForeignPtr (bufferMemory buffer) $ \ptr -> do
            let capacity = min metadataBufferBytes (bufferCapacity buffer)
            count <- receive ptr capacity
            validateCount capacity count
            pure (BI.fromForeignPtr (bufferMemory buffer) 0 count)
        takePending = atomicModifyIORef' pending (B.empty,)
        metadata = do
            saved <- takePending
            bytes <- if B.null saved then receiveLoan else pure saved
            initial <- readIORef lineHasContent
            let scan i hasContent
                    | i == B.length bytes = (i, hasContent)
                    | B.index bytes i == 10 =
                        if hasContent then scan (i + 1) False else (i + 1, False)
                    | B.index bytes i == 13 = scan (i + 1) hasContent
                    | otherwise = scan (i + 1) True
                (count, content) = scan 0 initial
                (header, readAhead) = B.splitAt count bytes
            owned <- E.evaluate (B.copy header)
            writeIORef lineHasContent content
            writeIORef pending readAhead
            when (B.null readAhead) release
            pure owned
        body supplied = do
            saved <- takePending
            if not (B.null saved) then pure saved else do
                release
                maybe receiveLoan receiveBuffer supplied
    source <- mkSource $ do
        current <- readIORef binding
        case current of
            Metadata -> metadata
            Idle -> invalid
            Body supplied -> body supplied
    pure (InputContext source binding loan pending)

headersComplete :: InputContext -> IO ()
headersComplete context = writeIORef (contextBinding context) Idle

releaseConsumedInput :: InputContext -> IO ()
releaseConsumedInput context = do
    leftover <- readLeftoverSource (contextSource context)
    pending <- readIORef (contextPrefix context)
    when (B.null leftover && B.null pending) $
        atomicModifyIORef' (contextLoan context) (Nothing,) >>= mapM_ releaseBuffer

-- At request completion any remaining bytes belong to a following request.
-- Own that single bounded pipeline prefix before retiring this capability.
inputLeftover :: InputContext -> IO B.ByteString
inputLeftover context = do
    leftover <- readLeftoverSource (contextSource context)
    pending <- readIORef (contextPrefix context)
    let prefix = if B.null leftover then pending else leftover
    unless (B.length prefix <= metadataBufferBytes) invalid
    E.evaluate (B.copy prefix)

closeInputContext :: InputContext -> IO ()
closeInputContext context = do
    writeIORef (contextBinding context) Idle
    leftoverSource (contextSource context) B.empty
    writeIORef (contextPrefix context) B.empty
    atomicModifyIORef' (contextLoan context) (Nothing,) >>= mapM_ releaseBuffer

-- The caller joins all user callbacks before returning from action. Preserve
-- only a bounded pipeline prefix, then forget every borrowed descriptor.
withInputBuffer :: InputContext -> Maybe Buffer -> IO a -> IO a
withInputBuffer context supplied action = E.mask $ \restore -> do
    mapM_ (\buffer -> unless (bufferCapacity buffer > 0) invalid) supplied
    current <- readIORef (contextBinding context)
    case current of
        Idle -> pure ()
        _ -> invalid
    writeIORef (contextBinding context) (Body supplied)
    let source = contextSource context
        detach = writeIORef (contextBinding context) Idle
        run = do
            result <- restore action
            owned <- inputLeftover context
            leftoverSource source owned
            writeIORef (contextPrefix context) B.empty
            atomicModifyIORef' (contextLoan context) (Nothing,) >>= mapM_ releaseBuffer
            pure result
    run `E.onException` closeInputContext context `E.finally` detach

validateCount :: Int -> Int -> IO ()
validateCount capacity count = unless (capacity > 0 && count >= 0 && count <= capacity) invalid

invalid :: IO a
invalid = ioError (userError "Warp operation input is inactive or transport receive count is invalid")
