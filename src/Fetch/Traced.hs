{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}

module Fetch.Traced
  ( TracedFetchT
  , TraceConfig(..)
  , defaultTraceConfig
  , FetchStats(..)
  , runTracedFetchT
  ) where

import Fetch.Class
import Fetch.Cache
import Fetch.Batched (FetchT(..), FetchEnv(..), runLoopWith)
import Fetch.Engine (RoundStats(..))

import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import Data.IORef
import Data.Time.Clock (NominalDiffTime, getCurrentTime, diffUTCTime)

-- | Callbacks for observing the batching process.
data TraceConfig m = TraceConfig
  { onRoundStart    :: Int -> Batches m -> m ()
    -- ^ Called before each round with round number and pending batches.
  , onRoundComplete :: Int -> RoundStats -> m ()
    -- ^ Called after each round.
  , onFetchComplete :: FetchStats -> m ()
    -- ^ Called when the entire computation finishes.
  }

-- | No-op trace config.
defaultTraceConfig :: Applicative m => TraceConfig m
defaultTraceConfig = TraceConfig
  { onRoundStart    = \_ _ -> pure ()
  , onRoundComplete = \_ _ -> pure ()
  , onFetchComplete = \_   -> pure ()
  }

-- | Aggregate stats for an entire FetchT computation.
data FetchStats = FetchStats
  { totalRounds        :: !Int
  , totalKeys          :: !Int
  , maxSourcesPerRound :: !Int
    -- ^ Peak number of distinct data sources dispatched in any single round.
  , totalTime          :: !NominalDiffTime
  } deriving (Eq, Show)

-- | A traced variant of FetchT. Same batching\/caching behavior,
-- just adds observability hooks.
--
-- This is a newtype over 'FetchT' so it shares the exact same
-- Applicative batching. The tracing happens at the runner level.
--
-- All instances are derived via @GeneralizedNewtypeDeriving@.
-- If you define your own newtype over 'FetchT', you can use the
-- same pattern:
--
-- @
-- {-\# LANGUAGE GeneralizedNewtypeDeriving, DerivingStrategies \#-}
--
-- newtype MyFetchT m a = MyFetchT (FetchT m a)
--   deriving newtype
--     ( Functor, Applicative, Monad
--     , MonadFail, MonadThrow, MonadCatch
--     , MonadFetch m
--     )
-- @
newtype TracedFetchT m a = TracedFetchT (FetchT m a)
  deriving newtype
    ( Functor, Applicative, Monad
    , MonadFail, MonadThrow, MonadCatch
    , MonadFetch m
    )

-- | Run with tracing. Fires callbacks at each round boundary.
runTracedFetchT :: Monad m
                => (forall x. m x -> IO x)
                -> (forall x. IO x -> m x)
                -> TraceConfig m
                -> TracedFetchT m a
                -> m (a, FetchStats)
runTracedFetchT lower lift tc (TracedFetchT action) = do
  cacheRef <- lift newCacheRef
  startTime <- lift getCurrentTime
  statsRef <- lift $ newIORef (FetchStats 0 0 0 0)

  let e = FetchEnv
        { fetchCache = cacheRef
        , fetchLower = lower
        , fetchLift  = lift
        }

      withRound n batches exec = do
        onRoundStart tc n batches
        rs <- exec
        lift $ modifyIORef' statsRef $ \s -> s
          { totalRounds  = totalRounds s + 1
          , totalKeys    = totalKeys s + roundKeys rs
          , maxSourcesPerRound = max (maxSourcesPerRound s) (roundSources rs)
          }
        onRoundComplete tc n rs

  a <- runLoopWith e withRound action

  endTime <- lift getCurrentTime
  lift $ modifyIORef' statsRef $ \s ->
    s { totalTime = diffUTCTime endTime startTime }
  stats <- lift $ readIORef statsRef

  onFetchComplete tc stats
  pure (a, stats)
