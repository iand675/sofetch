{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}

module Fetch.Traced
  ( TracedFetch
  , TraceConfig(..)
  , defaultTraceConfig
  , FetchStats(..)
  , runTracedFetch
  ) where

import Fetch.Class
import Fetch.Cache
import Fetch.Batched (Fetch(..), FetchConfig(..), FetchEnv(..), runLoopWith)
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

-- | Aggregate stats for an entire Fetch computation.
data FetchStats = FetchStats
  { totalRounds        :: !Int
  , totalKeys          :: !Int
  , maxSourcesPerRound :: !Int
    -- ^ Peak number of distinct data sources dispatched in any single round.
  , totalTime          :: !NominalDiffTime
  } deriving (Eq, Show)

-- | A traced variant of Fetch. Same batching\/caching behavior,
-- just adds observability hooks.
--
-- This is a newtype over 'Fetch' so it shares the exact same
-- Applicative batching. The tracing happens at the runner level.
--
-- All instances are derived via @GeneralizedNewtypeDeriving@.
-- If you define your own newtype over 'Fetch', you can use the
-- same pattern:
--
-- @
-- {-\# LANGUAGE GeneralizedNewtypeDeriving, DerivingStrategies \#-}
--
-- newtype MyFetch m a = MyFetch (Fetch m a)
--   deriving newtype
--     ( Functor, Applicative, Monad
--     , MonadFail, MonadThrow, MonadCatch
--     , MonadFetch m
--     )
-- @
newtype TracedFetch m a = TracedFetch (Fetch m a)
  deriving newtype
    ( Functor, Applicative, Monad
    , MonadFail, MonadThrow, MonadCatch
    , MonadFetch m
    )

-- | Run with tracing. Fires callbacks at each round boundary.
runTracedFetch :: Monad m
               => FetchConfig m
               -> TraceConfig m
               -> TracedFetch m a
               -> m (a, FetchStats)
runTracedFetch cfg tc (TracedFetch action) = do
  cacheRef <- case configCache cfg of
    Just ref -> pure ref
    Nothing  -> configLift cfg newCacheRef
  startTime <- configLift cfg getCurrentTime
  statsRef <- configLift cfg $ newIORef (FetchStats 0 0 0 0)

  let e = FetchEnv
        { fetchCache = cacheRef
        , fetchLower = configLower cfg
        , fetchLift  = configLift cfg
        }

      withRound n batches exec = do
        onRoundStart tc n batches
        rs <- exec
        configLift cfg $ modifyIORef' statsRef $ \s -> s
          { totalRounds  = totalRounds s + 1
          , totalKeys    = totalKeys s + roundKeys rs
          , maxSourcesPerRound = max (maxSourcesPerRound s) (roundSources rs)
          }
        onRoundComplete tc n rs

  a <- runLoopWith e withRound action

  endTime <- configLift cfg getCurrentTime
  configLift cfg $ modifyIORef' statsRef $ \s ->
    s { totalTime = diffUTCTime endTime startTime }
  stats <- configLift cfg $ readIORef statsRef

  onFetchComplete tc stats
  pure (a, stats)
