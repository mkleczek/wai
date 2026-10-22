module Network.Wai.Handler.Warp.Streaming.Gate
    ( OperationGate, newOperationGate, runOperation, closeOperationGate, unavailable ) where

import Control.Concurrent (ThreadId, myThreadId, throwTo)
import Control.Concurrent.MVar
import qualified Control.Exception as E
import Control.Monad (unless, when)
import Data.IORef

-- An operation may run once at a time and must finish before its borrow ends.
-- Recording its thread lets teardown cancel a blocked socket operation, rather
-- than returning storage while an escaped operation can still access it.
newtype OperationGate = OperationGate (IORef (Bool, Maybe (ThreadId, MVar ())))

newOperationGate :: IO OperationGate
newOperationGate = OperationGate <$> newIORef (True, Nothing)

unavailable :: IO a
unavailable = ioError (userError "Warp request capability is expired, mixed, or already used")

runOperation :: OperationGate -> IO a -> IO a
runOperation (OperationGate state) action = E.mask $ \restore -> do
    tid <- myThreadId
    done <- newEmptyMVar
    claimed <- atomicModifyIORef' state $ \old -> case old of
        (True, Nothing) -> ((True, Just (tid, done)), True)
        _ -> (old, False)
    unless claimed unavailable
    restore action `E.finally` do
        atomicModifyIORef' state $ \(live, _) -> ((live, Nothing), ())
        putMVar done ()

-- Return whether teardown found an unfinished operation. Invalidating first
-- rejects any operation racing the close. This join cannot itself be cancelled:
-- the owner is permitted to reuse the storage as soon as cleanup unwinds.
closeOperationGate :: OperationGate -> IO Bool
closeOperationGate (OperationGate state) = E.uninterruptibleMask_ $ do
    pending <- atomicModifyIORef' state $ \(_, busy) -> ((False, busy), busy)
    case pending of
        Nothing -> pure False
        Just (tid, done) -> do
            current <- myThreadId
            when (tid == current) unavailable
            throwTo tid E.ThreadKilled
            readMVar done
            pure True

