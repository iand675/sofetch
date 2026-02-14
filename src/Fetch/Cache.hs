{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

module Fetch.Cache
  ( CacheRef
  , newCacheRef
    -- * Lookup
  , CacheLookup(..)
  , cacheLookup
    -- * Allocation & writing
  , cacheAllocate
  , cacheAllocateForce
  , cacheInsert
  , cacheInsertError
    -- * Eviction
  , cacheEvict
  , cacheEvictSource
  , cacheEvictWhere
    -- * Warming & export
  , cacheWarm
  , cacheContents
  ) where

import Fetch.Class
import Fetch.IVar

import Data.Dynamic
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.IORef
import Type.Reflection (SomeTypeRep, someTypeRep)

-- | Internal representation: each entry is a Dynamic wrapping
-- @HashMap k (IVar (Result k))@.
type ResultMap = Map SomeTypeRep Dynamic

-- | A mutable, shared cache. Stores IVars so that in-flight
-- requests can be deduplicated and concurrent readers can
-- block on pending results.
newtype CacheRef = CacheRef (IORef ResultMap)

-- | Create an empty cache.
newCacheRef :: IO CacheRef
newCacheRef = CacheRef <$> newIORef Map.empty

-- ──────────────────────────────────────────────
-- Lookup
-- ──────────────────────────────────────────────

-- | Result of looking up a key in the cache.
data CacheLookup a
  = CacheMiss
    -- ^ Key has never been requested.
  | CacheHitPending (IVar a)
    -- ^ Key is being fetched by another round. Wait on the IVar.
  | CacheHitReady a
    -- ^ Key has a resolved value.

-- | Look up a key in the cache. Distinguishes between miss,
-- pending (in-flight), and ready (resolved).
cacheLookup :: forall k. (FetchKey k, Typeable (Result k))
            => CacheRef -> k -> IO (CacheLookup (Result k))
cacheLookup (CacheRef ref) k = do
  cache <- readIORef ref
  let trep = someTypeRep (Proxy @k)
  case Map.lookup trep cache >>= fromDynamic of
    Just (ivars :: HashMap k (IVar (Result k))) ->
      case HM.lookup k ivars of
        Just iv -> do
          mr <- tryReadIVar iv
          case mr of
            Just (Right v) -> pure (CacheHitReady v)
            Just (Left _)  -> pure CacheMiss
              -- Errored IVars are treated as a miss: allow retry.
            Nothing        -> pure (CacheHitPending iv)
        Nothing -> pure CacheMiss
    Nothing -> pure CacheMiss

-- ──────────────────────────────────────────────
-- Allocation
-- ──────────────────────────────────────────────

-- | Atomically allocate IVars for keys not already in the cache.
-- Returns only the newly allocated (key, IVar) pairs; keys that
-- already had IVars (filled or pending) are skipped.
--
-- This is the deduplication point: concurrent calls to
-- @cacheAllocate@ for the same key will only allocate once.
cacheAllocate :: forall k. (FetchKey k, Typeable (Result k))
              => CacheRef -> [k] -> IO [(k, IVar (Result k))]
cacheAllocate (CacheRef ref) keys = do
  -- Allocate IVars in IO before the atomic update. Some may be
  -- wasted if the key is already cached, but this avoids the
  -- unsafePerformIO-inside-atomicModifyIORef pitfall where GHC
  -- optimisations can break sharing of thunks.
  candidates <- mapM (\k -> do iv <- newIVar; pure (k, iv)) keys
  atomicModifyIORef' ref $ \cache ->
    let trep = someTypeRep (Proxy @k)
        existing :: HashMap k (IVar (Result k))
        existing = case Map.lookup trep cache >>= fromDynamic of
          Just m  -> m
          Nothing -> HM.empty

        go [] acc ivMap = (acc, ivMap)
        go ((k, iv):rest) acc ivMap =
          case HM.lookup k ivMap of
            Just _  -> go rest acc ivMap
            Nothing -> go rest ((k, iv) : acc) (HM.insert k iv ivMap)

        (newPairs, updated) = go candidates [] existing
        cache' = Map.insert trep (toDyn updated) cache
    in (cache', newPairs)

-- | Like 'cacheAllocate', but always creates fresh IVars,
-- overwriting any existing entries for the same keys.
--
-- Used by the engine for 'NoCaching' data sources. The old IVar
-- (if any) is replaced atomically, so:
--
-- * Continuations from the /current/ round all share the new IVar
--   (within-round deduplication is preserved).
-- * Continuations from a /prior/ round that already read the old
--   IVar are unaffected (they completed before the overwrite).
-- * The next round will overwrite again, ensuring the source is
--   re-fetched every time.
cacheAllocateForce :: forall k. (FetchKey k, Typeable (Result k))
                   => CacheRef -> [k] -> IO [(k, IVar (Result k))]
cacheAllocateForce (CacheRef ref) keys = do
  candidates <- mapM (\k -> do iv <- newIVar; pure (k, iv)) keys
  atomicModifyIORef' ref $ \cache ->
    let trep = someTypeRep (Proxy @k)
        existing :: HashMap k (IVar (Result k))
        existing = case Map.lookup trep cache >>= fromDynamic of
          Just m  -> m
          Nothing -> HM.empty

        -- Always overwrite: insert every candidate regardless of
        -- whether the key already has an IVar in the cache.
        updated = foldl (\m (k, iv) -> HM.insert k iv m) existing candidates
        cache' = Map.insert trep (toDyn updated) cache
    in (cache', candidates)

-- ──────────────────────────────────────────────
-- Writing
-- ──────────────────────────────────────────────

-- | Look up the IVar for a key and apply an action to it.
-- No-op if the key has no allocated IVar.
withCachedIVar :: forall k. (FetchKey k, Typeable (Result k))
               => CacheRef -> k -> (IVar (Result k) -> IO ()) -> IO ()
withCachedIVar (CacheRef ref) k action = do
  cache <- readIORef ref
  let trep = someTypeRep (Proxy @k)
  case Map.lookup trep cache >>= fromDynamic of
    Just (ivars :: HashMap k (IVar (Result k))) ->
      case HM.lookup k ivars of
        Just iv -> action iv
        Nothing -> pure ()
    Nothing -> pure ()

-- | Write a success result into a previously allocated IVar.
cacheInsert :: forall k. (FetchKey k, Typeable (Result k))
            => CacheRef -> k -> Result k -> IO ()
cacheInsert cRef k v = withCachedIVar cRef k $ \iv -> writeIVar iv v

-- | Write an error into a previously allocated IVar.
cacheInsertError :: forall k. (FetchKey k, Typeable (Result k))
                 => CacheRef -> k -> SomeException -> IO ()
cacheInsertError cRef k e = withCachedIVar cRef k $ \iv -> writeIVarError iv e

-- ──────────────────────────────────────────────
-- Eviction
-- ──────────────────────────────────────────────

-- | Evict a single key.
cacheEvict :: forall k. (FetchKey k, Typeable (Result k))
           => CacheRef -> k -> IO ()
cacheEvict (CacheRef ref) k = do
  let trep = someTypeRep (Proxy @k)
  atomicModifyIORef' ref $ \cache ->
    ( Map.adjust
        (\dyn ->
          let ivars = fromDyn dyn (HM.empty :: HashMap k (IVar (Result k)))
          in toDyn (HM.delete k ivars))
        trep cache
    , () )

-- | Evict all cached results for a data source.
cacheEvictSource :: forall k. (Typeable k)
                 => CacheRef -> Proxy k -> IO ()
cacheEvictSource (CacheRef ref) _ =
  atomicModifyIORef' ref $ \cache ->
    (Map.delete (someTypeRep (Proxy @k)) cache, ())

-- | Evict keys matching a predicate.
cacheEvictWhere :: forall k. (FetchKey k, Typeable (Result k))
                => CacheRef -> Proxy k -> (k -> Bool) -> IO ()
cacheEvictWhere (CacheRef ref) _ predicate = do
  let trep = someTypeRep (Proxy @k)
  atomicModifyIORef' ref $ \cache ->
    ( Map.adjust
        (\dyn ->
          let ivars = fromDyn dyn (HM.empty :: HashMap k (IVar (Result k)))
          in toDyn (HM.filterWithKey (\k' _ -> not (predicate k')) ivars))
        trep cache
    , () )

-- ──────────────────────────────────────────────
-- Warming & export
-- ──────────────────────────────────────────────

-- | Warm the cache with known values. Creates pre-filled IVars.
-- Useful for hydrating from an external cache (Redis, etc.)
-- at request start.
cacheWarm :: forall k. (FetchKey k, Typeable (Result k))
          => CacheRef -> HashMap k (Result k) -> IO ()
cacheWarm (CacheRef ref) values = do
  let trep = someTypeRep (Proxy @k)
  ivars <- HM.traverseWithKey (\_ v -> do
    iv <- newIVar
    writeIVar iv v
    pure iv) values
  atomicModifyIORef' ref $ \cache ->
    let existing :: HashMap k (IVar (Result k))
        existing = case Map.lookup trep cache >>= fromDynamic of
          Just m  -> m
          Nothing -> HM.empty
    in (Map.insert trep (toDyn (HM.union ivars existing)) cache, ())

-- | Read all resolved values for a source (for debugging/export).
cacheContents :: forall k. (FetchKey k, Typeable (Result k))
              => CacheRef -> Proxy k -> IO (HashMap k (Result k))
cacheContents (CacheRef ref) _ = do
  cache <- readIORef ref
  let trep = someTypeRep (Proxy @k)
  case Map.lookup trep cache >>= fromDynamic of
    Just (ivars :: HashMap k (IVar (Result k))) ->
      HM.mapMaybe id <$> traverse (\iv -> do
        mr <- tryReadIVar iv
        pure $ case mr of
          Just (Right v) -> Just v
          _              -> Nothing) ivars
    Nothing -> pure HM.empty
