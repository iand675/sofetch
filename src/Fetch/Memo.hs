{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Fetch.Memo
  ( MemoKey(..)
  , MemoStore
  , newMemoStore
  , memo
  , memoOn
  ) where

import Data.Dynamic
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Hashable (Hashable)
import Data.IORef
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Proxy
import Type.Reflection (SomeTypeRep, someTypeRep)

-- | A typed key for memoized computations.
class (Typeable k, Hashable k, Eq k) => MemoKey k where
  type MemoResult k

-- | A mutable store for memoized computation results.
-- Separate from the fetch cache (different lifetime, different concerns).
newtype MemoStore = MemoStore (IORef (Map SomeTypeRep Dynamic))

-- | Create an empty memo store.
newMemoStore :: IO MemoStore
newMemoStore = MemoStore <$> newIORef Map.empty

-- | Memoize a computation by key. If the key has been computed before
-- (in this MemoStore), return the cached result. Otherwise, run the
-- computation and cache it.
--
-- __Concurrency note:__ if two threads call @memo@ with the same key
-- concurrently and no cached value exists yet, both may execute the
-- action. One result will be stored (last writer wins); the other is
-- discarded. This is safe when the action is idempotent or pure, but
-- means @memo@ is /not/ a once-only guarantee; it is a best-effort
-- deduplication. If you need exactly-once semantics, synchronise
-- externally (e.g. with an 'MVar' per key).
memo :: forall k m. (MemoKey k, Typeable (MemoResult k), Monad m)
     => MemoStore
     -> (forall x. IO x -> m x)
     -> k
     -> m (MemoResult k)
     -> m (MemoResult k)
memo store toIO k = memoImpl store toIO (someTypeRep (Proxy @k)) k

-- | Memoize with an inline key type. Convenient for one-off
-- memoization without declaring a MemoKey instance.
--
-- Uses the result type for disambiguation, so beware of
-- ambiguous types.
--
-- Same concurrency caveat as 'memo': concurrent calls for the
-- same key may execute the action more than once.
memoOn :: forall k a m. (Typeable a, Typeable k, Hashable k, Monad m)
       => MemoStore
       -> (forall x. IO x -> m x)
       -> k
       -> m a
       -> m a
memoOn store toIO k =
  -- Key the store entry by (key type, result type) to disambiguate
  -- different result types sharing the same key type.
  memoImpl store toIO (someTypeRep (Proxy @(k, a))) k

-- | Shared memoization logic. Looks up @trep@ in the store,
-- returning the cached value if found, otherwise runs the action
-- and atomically inserts the result.
memoImpl :: forall k v m.
            (Typeable k, Typeable v, Hashable k, Monad m)
         => MemoStore
         -> (forall x. IO x -> m x)
         -> SomeTypeRep
         -> k
         -> m v
         -> m v
memoImpl (MemoStore ref) toIO trep k action = do
  existing <- toIO $ atomicModifyIORef' ref $ \store ->
    case Map.lookup trep store >>= fromDynamic of
      Just (hm :: HashMap k v) -> (store, HM.lookup k hm)
      Nothing                  -> (store, Nothing)
  case existing of
    Just v  -> pure v
    Nothing -> do
      v <- action
      -- Atomic insert: re-reads the current map to avoid clobbering
      -- concurrent writes to other keys.
      toIO $ atomicModifyIORef' ref $ \store ->
        let hm :: HashMap k v
            hm = case Map.lookup trep store >>= fromDynamic of
              Just m  -> m
              Nothing -> HM.empty
        in (Map.insert trep (toDyn (HM.insert k v hm)) store, ())
      pure v
