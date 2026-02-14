{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | OpenTelemetry instrumentation for sofetch.
--
-- Creates a root span @sofetch.fetch@ covering the entire computation,
-- with child spans @sofetch.round@ wrapping each batch round.
--
-- When no 'TracerProvider' is configured (the default), all tracing
-- operations are no-ops with negligible overhead.
--
-- @
-- import OpenTelemetry.Trace.Core (makeTracer, tracerOptions)
--
-- tracer <- makeTracer tracerProvider "sofetch" tracerOptions
-- result <- runFetchTWithOTel tracer id liftIO myFetchAction
-- @
module Fetch.OpenTelemetry
  ( runFetchTWithOTel
  , runFetchTWithCacheWithOTel
  ) where

import Fetch
  ( FetchT, FetchEnv(..)
  , CacheRef, newCacheRef
  , Batches, batchSourceCount, batchSize
  , RoundStats(..)
  , runLoopWith
  )

import Data.IORef
import Data.Int (Int64)
import OpenTelemetry.Trace.Core
  ( Tracer
  , SpanArguments(..), SpanKind(..), defaultSpanArguments
  , addAttribute, inSpan'
  )

-- | Run a 'FetchT' computation with OpenTelemetry instrumentation.
--
-- Creates:
--
-- * A root span @sofetch.fetch@ covering the entire computation.
-- * A child span @sofetch.round@ for each batch round, wrapping
--   the actual batch execution (not just the scheduling).
--
-- Root span attributes:
--
-- * @sofetch.total_rounds@: number of batch rounds executed
-- * @sofetch.total_keys@: total keys fetched across all rounds
-- * @sofetch.max_sources_per_round@: max distinct data sources in any round
--
-- Round span attributes:
--
-- * @sofetch.round.number@: 1-based round index
-- * @sofetch.round.sources@: data sources dispatched this round
-- * @sofetch.round.keys@: keys dispatched this round
-- * @sofetch.round.cache_hits@: keys served from cache this round
--
-- This variant takes two natural transformations to bridge @m@ and @IO@.
-- For @m ~ IO@, use @id@ for both.
runFetchTWithOTel :: forall m a.
                     Monad m
                  => Tracer
                  -> (forall x. m x -> IO x)
                  -> (forall x. IO x -> m x)
                  -> FetchT m a
                  -> m a
runFetchTWithOTel tracer lower lift action = do
  cRef <- lift newCacheRef
  runFetchTWithCacheWithOTel tracer lower lift cRef action

-- | Like 'runFetchTWithOTel' but with an externally managed 'CacheRef',
-- allowing cache sharing across multiple runs.
runFetchTWithCacheWithOTel
  :: forall m a. Monad m
  => Tracer
  -> (forall x. m x -> IO x)
  -> (forall x. IO x -> m x)
  -> CacheRef
  -> FetchT m a
  -> m a
runFetchTWithCacheWithOTel tracer lower lift cRef action =
  lift $ inSpan' tracer "sofetch.fetch" defaultSpanArguments { kind = Internal } $ \rootSpan -> do
    statsRef <- newIORef (0 :: Int, 0 :: Int, 0 :: Int)

    let e = FetchEnv
          { fetchCache = cRef
          , fetchLower = lower
          , fetchLift  = lift
          }

        -- Wrap each round in an OTel span, then accumulate stats.
        withRound :: Int -> Batches m -> m RoundStats -> m ()
        withRound !n batches exec = do
          let nSources = batchSourceCount batches
              nKeys    = batchSize batches
          rs <- lift $ inSpan' tracer "sofetch.round" defaultSpanArguments { kind = Internal } $ \roundSpan -> do
            addAttribute roundSpan "sofetch.round.number"  (fromIntegral n :: Int64)
            addAttribute roundSpan "sofetch.round.sources" (fromIntegral nSources :: Int64)
            addAttribute roundSpan "sofetch.round.keys"    (fromIntegral nKeys :: Int64)
            rs <- lower exec
            addAttribute roundSpan "sofetch.round.cache_hits" (fromIntegral (roundCacheHits rs) :: Int64)
            pure rs
          lift $ modifyIORef' statsRef $ \(r, k', s) ->
            (r + 1, k' + roundKeys rs, max s (roundSources rs))

    result <- lower $ runLoopWith e withRound action

    (rounds, keys, sources) <- readIORef statsRef
    addAttribute rootSpan "sofetch.total_rounds"          (fromIntegral rounds  :: Int64)
    addAttribute rootSpan "sofetch.total_keys"            (fromIntegral keys    :: Int64)
    addAttribute rootSpan "sofetch.max_sources_per_round" (fromIntegral sources :: Int64)

    pure result
