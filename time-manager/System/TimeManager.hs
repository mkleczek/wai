{-# LANGUAGE CPP #-}
{-# LANGUAGE RecordWildCards #-}

-- | Timeout manager. Since @v0.3.0@, timeout manager is a wrapper of
-- GHC System TimerManager.
--
-- Some caveats of using this package:
--
--   * Only works for GHC
--   * Only works with a threaded runtime
--   * Users of older versions should check the current semantics.
--   * Using 32-bit systems means the max timeout is @'maxBound' :: Int@
--     (2147483647) microseconds, which is less than 36 minutes.
--   * Using the same 'Handle' in different threads might cause issues in some
--     edge cases. (i.e. using cancel/pause in one thread, and resume in another)
module System.TimeManager (
    -- ** Types
    Manager,
    defaultManager,
    TimeoutAction,
    Handle,
    emptyHandle,

    -- ** Manager
    initialize,
    stopManager,
    killManager,
    withManager,
    withManager',

    -- ** Registering a timeout action
    withHandle,
    withHandleKillThread,

    -- ** Control timeout
    tickle,
    pause,
    resume,

    -- ** Low level
    register,
    registerKillThread,
    cancel,

    -- ** Exceptions
    TimeoutThread (..),
) where

import Control.Concurrent (forkIO, mkWeakThreadId, myThreadId)
import qualified Control.Exception as E
import Control.Monad (void, when)
import qualified Data.IORef as I
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import System.Mem.Weak (deRefWeak)
import System.TimeManager.Internal

#if defined(mingw32_HOST_OS)
import qualified GHC.Event.Windows as EV
#else
import qualified GHC.Event as EV
#endif

----------------------------------------------------------------

-- | A manager whose timeout value is 0 (no callbacks are fired).
defaultManager :: Manager
defaultManager = Manager 0

----------------------------------------------------------------

-- | Dummy 'Handle'.
emptyHandle :: Handle
emptyHandle =
    Handle
        { handleTimeout = 0
        , handleAction = pure ()
        , handleKeyRef = error "time-manager: Handle.handleKeyRef not set"
        , handleState = error "time-manager: Handle.handleState not set"
        }

----------------------------------------------------------------

-- | Creating timeout manager with a timeout value in microseconds.
--
--   Setting the timeout to zero or lower (<= 0) will produce a
--   `defaultManager`.
initialize :: Int -> IO Manager
initialize = pure . Manager . max 0

----------------------------------------------------------------

-- | Obsoleted since version 0.3.0
--   Is now equivalent to @pure ()@.
stopManager :: Manager -> IO ()
stopManager _ = pure ()
{-# DEPRECATED stopManager "This function does nothing since version 0.3.0" #-}

-- | Obsoleted since version 0.3.0
--   Is now equivalent to @pure ()@.
killManager :: Manager -> IO ()
killManager _ = pure ()
{-# DEPRECATED killManager "This function does nothing since version 0.3.0" #-}

----------------------------------------------------------------

-- | Registering a timeout action and unregister its handle
--   when the body action is finished.
withHandle :: Manager -> TimeoutAction -> (Handle -> IO a) -> IO a
withHandle mgr onTimeout action
    | isNoManager mgr = action emptyHandle
    | otherwise = E.bracket (register mgr onTimeout) cancel action

-- | Registering a timeout action of killing this thread and
--   unregister its handle when the body action is killed or finished.
withHandleKillThread :: Manager -> TimeoutAction -> (Handle -> IO ()) -> IO ()
withHandleKillThread mgr onTimeout action
    | isNoManager mgr = action emptyHandle
    | otherwise =
        E.handle ignore $ E.bracket (registerKillThread mgr onTimeout) cancel action
  where
    ignore TimeoutThread = pure ()

----------------------------------------------------------------

-- | Registering a timeout action.
register :: Manager -> TimeoutAction -> IO Handle
register mgr@(Manager timeout) onTimeout
    | isNoManager mgr = pure emptyHandle
    | otherwise = do
        keyref <- I.newIORef $ error "time-manager: Handle.handleKeyRef not set"
        state <- I.newIORef . HandleState True =<< deadlineAfter timeout
        let h =
                Handle
                    { handleTimeout = timeout
                    , handleAction = onTimeout
                    , handleKeyRef = keyref
                    , handleState = state
                    }
        arm timeout h
        pure h

-- | Registering the GHC timer that checks the deadline of a handle.
arm :: Int -> Handle -> IO ()
arm micros h@Handle{..} = do
    mgr <- getTimerManager
    key <- EV.registerTimeout mgr micros (expire h)
    I.atomicWriteIORef handleKeyRef key

data Verdict = Dormant | Expired | Later Word64

-- | A fired timer does not mean a timeout: the deadline may have been pushed
-- back by 'tickle' or 'resume', or removed by 'pause'.
expire :: Handle -> IO ()
expire h@Handle{..} = do
    now <- getMonotonicTimeNSec
    verdict <- I.atomicModifyIORef' handleState $ \st@HandleState{..} ->
        if handleDeadline == 0
            then (HandleState False 0, Dormant)
            else
                if handleDeadline > now
                    then (st, Later (handleDeadline - now))
                    else (HandleState False 0, Expired)
    case verdict of
        Dormant -> pure ()
        Expired -> handleAction
        Later nanos -> arm (fromIntegral (max 1 ((nanos + 999) `div` 1000))) h

deadlineAfter :: Int -> IO Word64
deadlineAfter micros = (+ fromIntegral micros * 1000) <$> getMonotonicTimeNSec

-- | Unregistering the timeout.
cancel :: Handle -> IO ()
cancel h@Handle{..} = withNonEmptyHandle h $ do
    armed <- I.atomicModifyIORef' handleState $ \HandleState{..} -> (HandleState False 0, handleArmed)
    when armed $ do
        mgr <- getTimerManager
        key <- I.readIORef handleKeyRef
        EV.unregisterTimeout mgr key

-- | Extending the timeout.
--
-- Careful: this does NOT reactivate an already paused 'Handle'!
tickle :: Handle -> IO ()
tickle h@Handle{..} = withNonEmptyHandle h $ do
    deadline <- deadlineAfter handleTimeout
    I.atomicModifyIORef' handleState $ \st ->
        (if handleDeadline st == 0 then st else st{handleDeadline = deadline}, ())

-- | Stopping the timeout until 'resume' is called. The registered timer
-- stays and is ignored when it fires.
pause :: Handle -> IO ()
pause h@Handle{..} = withNonEmptyHandle h $
    I.atomicModifyIORef' handleState $ \st -> (st{handleDeadline = 0}, ())

-- | Resuming the timeout.
--
-- Works like 'tickle' if the 'Handle' wasn't 'pause'd or 'cancel'ed.
resume :: Handle -> IO ()
resume h@Handle{..} = withNonEmptyHandle h $ do
    deadline <- deadlineAfter handleTimeout
    needTimer <- I.atomicModifyIORef' handleState $ \HandleState{..} ->
        (HandleState True deadline, not handleArmed)
    when needTimer $ arm handleTimeout h

----------------------------------------------------------------

-- | The asynchronous exception thrown if a thread is registered via
-- 'registerKillThread'.
data TimeoutThread = TimeoutThread

instance E.Exception TimeoutThread where
    toException = E.asyncExceptionToException
    fromException = E.asyncExceptionFromException

instance Show TimeoutThread where
    show TimeoutThread = "Thread killed by timeout manager"

-- | Registering a timeout action of killing this thread.
--   'TimeoutThread' is thrown to the thread which called this
--   function on timeout. Catch 'TimeoutThread' if you don't
--   want to leak the asynchronous exception to GHC RTS.
registerKillThread :: Manager -> TimeoutAction -> IO Handle
registerKillThread m onTimeout = do
    wtid <- myThreadId >>= mkWeakThreadId
    -- First run the timeout action in case the child thread is masked.
    register m $
        onTimeout `E.finally` do
            mtid <- deRefWeak wtid
            case mtid of
                Nothing -> pure ()
                Just tid' -> void . forkIO $ E.throwTo tid' TimeoutThread

----------------------------------------------------------------

-- | Call the inner function with a timeout manager.
withManager
    :: Int
    -- ^ timeout in microseconds
    -> (Manager -> IO a)
    -> IO a
withManager timeout f = initialize timeout >>= f

-- | Call the inner function with a timeout manager.
--   This is identical to 'withManager'.
withManager'
    :: Int
    -- ^ timeout in microseconds
    -> (Manager -> IO a)
    -> IO a
withManager' = withManager
{-# DEPRECATED withManager' "This function is the same as 'withManager' since version 0.3.0" #-}
