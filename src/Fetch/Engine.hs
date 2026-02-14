{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

module Fetch.Engine
  ( executeBatches
  , RoundStats(..)
  , emptyRoundStats
  ) where

import Fetch.Class
import Fetch.Cache
import Fetch.IVar

import Control.Concurrent.Async (async, wait)
import Control.Exception (try, toException)
import Control.Monad (unless)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.HashSet (HashSet)
import qualified Data.HashSet as HS
import qualified Data.Map.Strict as Map

-- | Stats for a single round of fetching.
data RoundStats = RoundStats
  { roundSources   :: !Int  -- ^ Number of distinct data sources
  , roundKeys      :: !Int  -- ^ Total (deduplicated) keys fetched
  , roundCacheHits :: !Int  -- ^ Keys found in cache (skipped)
  } deriving (Eq, Show)

emptyRoundStats :: RoundStats
emptyRoundStats = RoundStats 0 0 0

-- | Execute all batches in a round. Respects FetchStrategy ordering:
--
-- 1. EagerStart sources dispatched first (async, high-latency head start)
-- 2. Sequential sources dispatched one at a time (blocking)
-- 3. Concurrent sources dispatched in parallel (async)
-- 4. All async handles awaited
--
-- The two natural transformations bridge the source monad @m@ with @IO@:
--
-- * @lower@ runs @m@ actions in @IO@ (for dispatching @batchFetch@).
-- * @liftM@ lifts @IO@ actions into @m@ (for IVar writes inside
--   streaming callbacks).
executeBatches :: forall m.
                  (forall x. m x -> IO x)
               -> (forall x. IO x -> m x)
               -> CacheRef
               -> Batches m
               -> IO RoundStats
executeBatches lower liftM cacheRef (Batches batchMap) = do
  let batches = Map.elems batchMap
      (eager, sequential, concurrent) = partitionBatches batches

  -- Phase 1: Start eager sources first (async, high-latency head start)
  eagerHandles <- mapM (async . dispatchBatch lower liftM cacheRef) eager

  -- Phase 2: Run sequential sources synchronously
  seqHits <- sum <$> mapM (dispatchBatch lower liftM cacheRef) sequential

  -- Phase 3: Start concurrent sources in parallel
  concurrentHandles <- mapM (async . dispatchBatch lower liftM cacheRef) concurrent

  -- Phase 4: Wait for all async work
  eagerHits <- sum <$> mapM wait eagerHandles
  concurrentHits <- sum <$> mapM wait concurrentHandles

  let totalKeys = sum (fmap someBatchSize batches)
      hits = eagerHits + seqHits + concurrentHits
  pure RoundStats
    { roundSources   = length batches
    , roundKeys      = totalKeys
    , roundCacheHits = hits
    }

-- | Number of keys in a single existentially-wrapped batch.
someBatchSize :: SomeBatch m -> Int
someBatchSize (SomeBatch ks) = HS.size ks

-- | Partition batches by their fetch strategy.
partitionBatches :: forall m. [SomeBatch m] -> ([SomeBatch m], [SomeBatch m], [SomeBatch m])
partitionBatches = go [] [] []
  where
    go e s c [] = (reverse e, reverse s, reverse c)
    go e s c (b@(SomeBatch (_ :: HashSet k)) : rest) =
      case fetchStrategy @m @k Proxy of
        EagerStart  -> go (b : e) s c rest
        Sequential  -> go e (b : s) c rest
        Concurrent  -> go e s (b : c) rest

-- | Dispatch a single batch: allocate IVars for new keys, call
-- streamingFetch, and fill IVars with results. If the source throws,
-- fill all unfilled IVars with the exception.
-- Returns the number of cache hits (keys already in cache).
dispatchBatch :: forall m. (forall x. m x -> IO x) -> (forall x. IO x -> m x) -> CacheRef -> SomeBatch m -> IO Int
dispatchBatch lower liftM cacheRef (SomeBatch (ks :: HashSet k)) = do
  let policy = cachePolicy @m @k Proxy
  case policy of
    CacheResults -> dispatchCached lower liftM cacheRef ks
    NoCaching    -> dispatchUncached lower liftM cacheRef ks

-- | Normal path: allocate IVars, skip already-cached keys, fetch the rest.
-- Returns the number of cache hits (keys that were already cached).
dispatchCached :: forall m k. (DataSource m k, Typeable (Result k))
               => (forall x. m x -> IO x) -> (forall x. IO x -> m x)
               -> CacheRef -> HashSet k -> IO Int
dispatchCached lower liftM cacheRef ks = do
  let totalRequested = HS.size ks
  newPairs <- cacheAllocate cacheRef (HS.toList ks)
  let hits = totalRequested - length newPairs
  runStreamingFetch lower liftM newPairs
  pure hits

-- | Uncached path for 'NoCaching' data sources.
--
-- IVars are still placed in the cache for result delivery /within/
-- this round (so that deduplicated continuations from @\<*\>@ can
-- all read the same value via 'lookupOrAwait'). However, results
-- do not persist across rounds:
--
-- * 'cacheAllocateForce' overwrites any stale IVars left by a
--   prior round, so the continuation always reads a fresh result.
-- * @fetch@\/@tryFetch@ skip the cache check for 'NoCaching' keys,
--   so a subsequent round always returns 'Blocked' and re-enters
--   this dispatch path.
--
-- Together, these two mechanisms guarantee that the data source is
-- called once per round the key appears in, while within-round
-- deduplication is preserved.
dispatchUncached :: forall m k. (DataSource m k, Typeable (Result k))
                 => (forall x. m x -> IO x) -> (forall x. IO x -> m x)
                 -> CacheRef -> HashSet k -> IO Int
dispatchUncached lower liftM cacheRef ks = do
  newPairs <- cacheAllocateForce cacheRef (HS.toList ks)
  runStreamingFetch lower liftM newPairs
  pure 0

-- | Run streamingFetch for newly allocated IVars and fill any
-- missing or errored ones. No-op when the pair list is empty
-- (all keys were already cached or in-flight).
runStreamingFetch :: forall m k. DataSource m k
                  => (forall x. m x -> IO x)
                  -> (forall x. IO x -> m x)
                  -> [(k, IVar (Result k))]
                  -> IO ()
runStreamingFetch _ _ [] = pure ()
runStreamingFetch lower liftM (p : ps) = do
  let newPairs = p : ps
      ivarMap = HM.fromList newPairs
      newKeys = fst p :| map fst ps
  -- streamingFetch runs in m; the callback lifts IO IVar writes into m.
  -- We lower the whole m action to IO.
  result <- try $ lower $ streamingFetch @m @k newKeys $ \k v ->
    case HM.lookup k ivarMap of
      Just iv -> liftM $ writeIVar iv v
      Nothing -> pure ()
  case result of
    Right () -> fillUnfilled missingKeyError ivarMap
    Left ex  -> fillUnfilled ex ivarMap

-- | Fill all unfilled IVars with an error.
fillUnfilled :: SomeException -> HashMap k (IVar a) -> IO ()
fillUnfilled ex = mapM_ $ \iv -> do
  filled <- isIVarFilled iv
  unless filled $ writeIVarError iv ex

missingKeyError :: SomeException
missingKeyError = toException $ FetchError "Key not returned by data source"
