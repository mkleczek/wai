{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}

module System.TimeManager.Internal where

import Data.IORef (IORef)
import Data.Word (Word64)

#if defined(mingw32_HOST_OS)
import qualified GHC.Event.Windows as EV
#else
import qualified GHC.Event as EV
#endif

----------------------------------------------------------------

-- | A timeout manager
newtype Manager = Manager Int

isNoManager :: Manager -> Bool
isNoManager (Manager 0) = True
isNoManager _ = False

----------------------------------------------------------------

-- | An action (callback) to be performed on timeout.
type TimeoutAction = IO ()

-- | A handle used by a timeout manager.
data Handle = Handle
    { handleTimeout :: Int
    , handleAction :: TimeoutAction
    , handleKeyRef :: ~(IORef EV.TimeoutKey)
    , handleState :: ~(IORef HandleState)
    }

-- | Tracking the state of a handle, to be able to have 'resume'
-- act like a 'register' or 'tickle'.
--
-- A GHC timer is registered while the handle is armed. It only checks the
-- deadline (in monotonic nanoseconds, zero while paused) when it fires, so that
-- 'tickle', 'pause' and 'resume' leave GHC's single shared timer queue alone.
data HandleState = HandleState
    { handleArmed :: Bool
    , handleDeadline :: Word64
    }

isEmptyHandle :: Handle -> Bool
isEmptyHandle Handle{..} = handleTimeout == 0

withNonEmptyHandle :: Handle -> IO () -> IO ()
withNonEmptyHandle h act =
    if isEmptyHandle h then pure () else act

#if defined(mingw32_HOST_OS)
getTimerManager :: IO EV.Manager
getTimerManager = EV.getSystemManager
#else
getTimerManager :: IO EV.TimerManager
getTimerManager = EV.getSystemTimerManager
#endif
