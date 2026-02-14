{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module Fetch.IVar
  ( IVar
  , newIVar
  , writeIVar
  , writeIVarError
  , tryReadIVar
  , awaitIVar
  , isIVarFilled
  , FetchError(..)
  ) where

import Control.Concurrent.MVar
import Control.Exception (Exception, SomeException)
import Control.Monad (void)

-- | A write-once variable with error support.
--
-- Used internally by the cache to track in-flight fetches.
-- Reading blocks until the IVar is filled. Writing is idempotent:
-- only the first write takes effect.
newtype IVar a = IVar
  { ivarResult :: MVar (Either SomeException a)
    -- ^ Blocks readers until filled.
  }

-- | Create an empty IVar.
newIVar :: IO (IVar a)
newIVar = IVar <$> newEmptyMVar

-- | Non-blocking: is this IVar already resolved?
isIVarFilled :: IVar a -> IO Bool
isIVarFilled = fmap not . isEmptyMVar . ivarResult

-- | Non-blocking read. Returns 'Nothing' if not yet filled.
tryReadIVar :: IVar a -> IO (Maybe (Either SomeException a))
tryReadIVar = tryReadMVar . ivarResult

-- | Blocking read. Waits until the IVar is filled.
awaitIVar :: IVar a -> IO (Either SomeException a)
awaitIVar = readMVar . ivarResult

-- | Fill with a success value. Only the first write wins.
writeIVar :: IVar a -> a -> IO ()
writeIVar iv a = void $ tryPutMVar (ivarResult iv) (Right a)

-- | Fill with an error. Only the first write wins.
writeIVarError :: IVar a -> SomeException -> IO ()
writeIVarError iv e = void $ tryPutMVar (ivarResult iv) (Left e)

-- | Errors produced by the fetch engine itself (not by data sources).
newtype FetchError = FetchError String
  deriving stock (Eq, Show)
  deriving anyclass (Exception)
