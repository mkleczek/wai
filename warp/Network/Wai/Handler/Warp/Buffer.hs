module Network.Wai.Handler.Warp.Buffer (
    createWriteBuffer,
    ensureWriteBuffer,
    allocateBuffer,
    freeBuffer,
    toBuilderBuffer,
    bufferIO,
) where

import Control.Exception (mask_)
import Data.IORef (IORef, readIORef, writeIORef)
import qualified Data.Streaming.ByteString.Builder.Buffer as B (Buffer (..))
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc (free, mallocBytes)
import Foreign.Ptr (plusPtr)
import Network.Socket.BufferPool

import Network.Wai.Handler.Warp.Imports
import Network.Wai.Handler.Warp.Types

----------------------------------------------------------------

-- | Allocate a buffer of the given size and wrap it in a 'WriteBuffer'
-- containing that size and a finalizer.
createWriteBuffer :: BufSize -> IO WriteBuffer
createWriteBuffer size = do
    bytes <- allocateBuffer size
    return
        WriteBuffer
            { bufBuffer = bytes
            , bufSize = size
            , bufFree = freeBuffer bytes
            }

----------------------------------------------------------------

-- | Compatibility output is allocated only when a Builder/file path needs it.
-- The caller owns the response writer exclusively.
ensureWriteBuffer :: Int -> IORef WriteBuffer -> IO WriteBuffer
ensureWriteBuffer size ref = mask_ $ do
    current <- readIORef ref
    if bufSize current > 0 then pure current else do
        fresh <- createWriteBuffer (max 1 size)
        writeIORef ref fresh
        pure fresh

-- | Allocating a buffer with malloc().
allocateBuffer :: Int -> IO Buffer
allocateBuffer = mallocBytes

-- | Releasing a buffer with free().
freeBuffer :: Buffer -> IO ()
freeBuffer = free

----------------------------------------------------------------
--
-- Utilities
--

toBuilderBuffer :: IORef WriteBuffer -> IO B.Buffer
toBuilderBuffer writeBufferRef = do
    writeBuffer <- ensureWriteBuffer 16384 writeBufferRef
    let ptr = bufBuffer writeBuffer
        size = bufSize writeBuffer
    fptr <- newForeignPtr_ ptr
    return $ B.Buffer fptr ptr ptr (ptr `plusPtr` size)

bufferIO :: Buffer -> Int -> (ByteString -> IO ()) -> IO ()
bufferIO ptr siz io = do
    fptr <- newForeignPtr_ ptr
    io $ PS fptr 0 siz
