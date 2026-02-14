{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}

module Fetch.Class
  ( -- * Keys
    FetchKey(..)
    -- * Data sources
  , DataSource(..)
  , FetchStrategy(..)
  , CachePolicy(..)
    -- * Batching protocol
  , Status(..)
  , Batches(..)
  , SomeBatch(..)
  , singletonBatch
  , batchKeys
  , batchSize
  , batchSourceCount
  , mapStatus
    -- * MonadFetch
  , MonadFetch(..)
    -- * MonadFetchBatch
  , MonadFetchBatch(..)
    -- * Mutations
  , MutationKey(..)
    -- * Re-exports
  , Typeable
  , Hashable
  , NonEmpty(..)
  , Proxy(..)
  , SomeException
  ) where

import Control.Exception (SomeException)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.HashSet (HashSet)
import qualified Data.HashSet as HS
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Hashable (Hashable)
import Data.Proxy (Proxy(..))
import Data.Typeable (Typeable)
import Type.Reflection (SomeTypeRep, someTypeRep, eqTypeRep, typeRep, (:~~:)(HRefl))

-- ──────────────────────────────────────────────
-- Keys
-- ──────────────────────────────────────────────

-- | A typed key for a data fetch. The associated type family
-- pairs each key type with its result type, replacing Haxl's
-- GADT-based approach.
class (Typeable k, Hashable k, Eq k, Show k) => FetchKey k where
  type Result k

-- ──────────────────────────────────────────────
-- Data sources
-- ──────────────────────────────────────────────

-- | How the engine should schedule this source relative to others.
data FetchStrategy
  = Sequential  -- ^ Block on this source before starting others.
  | Concurrent  -- ^ Run alongside other sources (default).
  | EagerStart  -- ^ Start before sequential sources to overlap latency.
  deriving (Eq, Show)

-- | Whether results should be cached across rounds.
--
-- The default is 'CacheResults', which means a key fetched in round
-- /N/ is remembered: if the same key appears again in round /N+1/
-- (or anywhere later in the computation), the cached value is returned
-- immediately without dispatching a new batch.
--
-- 'NoCaching' opts out of this. Every round that mentions the key
-- dispatches a fresh batch to the data source, even if the key was
-- fetched moments ago. This is the right choice for data sources
-- whose results are non-idempotent or time-sensitive: counters,
-- "current timestamp" endpoints, queue-drain operations, etc.
--
-- __Within a single round__, deduplication still applies regardless
-- of policy: @(\',\') \<$\> fetch k \<*\> fetch k@ hits the source
-- once and both sides see the same value. The "no caching" guarantee
-- is strictly /across/ round boundaries introduced by @(>>=)@.
data CachePolicy
  = CacheResults
    -- ^ Cache results (default). Use for idempotent reads.
    -- A key is fetched at most once per 'CacheRef' lifetime.
  | NoCaching
    -- ^ Do not cache across rounds. The data source is re-fetched
    -- every round the key appears in. Within a single round,
    -- duplicate keys are still deduplicated.
  deriving (Eq, Show)

-- | A data source knows how to batch-fetch keys given capabilities
-- provided by the monad @m@.
--
-- The @m@ parameter replaces both the old @env@ parameter and
-- the concrete @IO@ in @batchFetch@. If your data source needs a
-- connection pool, @m@ should be a monad with access to one
-- (e.g. via 'MonadReader' or a newtype over 'ReaderT').
--
-- Type safety: if there is no @DataSource m k@ instance, code that
-- tries to @fetch@ a @k@ won't compile. No runtime errors from
-- missing config.
--
-- Libraries that use a restricted monad (e.g. a @Transaction@ type
-- that deliberately hides @MonadIO@) should export a convenience
-- runner:
--
-- @
-- fetchInTransaction :: Fetch Transaction a -> Transaction a
-- fetchInTransaction = runFetch unsafeRunTransaction unsafeLiftIO
-- @
--
-- This keeps the unsafe escape hatches private while giving users
-- a safe, typed entry point.
class (FetchKey k, Monad m) => DataSource m k where
  -- | Fetch a batch of keys. Must return a result for every key provided.
  -- The engine handles concurrency, error wrapping, and caching.
  --
  -- The list is guaranteed non-empty by the engine (a batch exists
  -- only because at least one @fetch@ blocked on it).
  --
  -- If your data source has no native batch API, implement 'fetchOne'
  -- instead; the default 'batchFetch' calls it for each key.
  batchFetch :: NonEmpty k -> m (HashMap k (Result k))
  batchFetch keys = HM.fromList . NE.toList <$> traverse (\k -> fmap (\v -> (k, v)) (fetchOne k)) keys

  -- | Fetch a single key. Override this for data sources that don't
  -- support batch operations; the default 'batchFetch' will call it
  -- for each key and assemble the results.
  --
  -- The default implementation delegates to @'batchFetch' (k :| [])@
  -- and extracts the result. You must implement at least one of
  -- 'batchFetch' or 'fetchOne'.
  --
  -- @
  -- instance DataSource AppM UserId where
  --   fetchOne (UserId uid) = lookupUserById uid
  -- @
  fetchOne :: k -> m (Result k)
  fetchOne k = do
    hm <- batchFetch (k :| [])
    case HM.lookup k hm of
      Just v  -> pure v
      Nothing -> error $
        "DataSource.fetchOne: batchFetch did not return key: " <> show k

  -- __Important:__ you must implement at least one of 'batchFetch' or
  -- 'fetchOne'. The defaults are mutually recursive: if neither is
  -- overridden, calls will loop at runtime. The @MINIMAL@ pragma
  -- below emits a compile-time warning, but it is not a hard error
  -- unless @-Werror@ is enabled.
  {-# MINIMAL batchFetch | fetchOne #-}

  -- | Optional: streaming fetch for sources where results arrive
  -- incrementally (gRPC streams, websockets, etc.).
  --
  -- Default delegates to 'batchFetch'.
  streamingFetch :: NonEmpty k -> (k -> Result k -> m ()) -> m ()
  streamingFetch ks callback = do
    results <- batchFetch ks
    _ <- HM.traverseWithKey callback results
    pure ()

  -- | How should the engine schedule this source?
  fetchStrategy :: Proxy k -> FetchStrategy
  fetchStrategy _ = Concurrent

  -- | Should results be cached across rounds?
  --
  -- Override to 'NoCaching' for data sources whose results are
  -- non-idempotent or time-sensitive. With 'NoCaching':
  --
  -- * @fetch@ never returns a cached value; it always blocks and
  --   waits for a fresh batch dispatch.
  -- * Within a single applicative round, duplicate keys are still
  --   deduplicated (one batch call, one result shared by all
  --   continuations).
  -- * Across @(>>=)@ round boundaries, each round dispatches a
  --   fresh batch, and the data source is called again.
  --
  -- @
  -- instance DataSource AppM CurrentTime where
  --   fetchOne _ = liftIO getCurrentTime
  --   cachePolicy _ = NoCaching
  -- @
  cachePolicy :: Proxy k -> CachePolicy
  cachePolicy _ = CacheResults

  -- | Human-readable name for tracing/logging.
  dataSourceName :: Proxy k -> String
  dataSourceName _ = show (someTypeRep (Proxy @k))

-- ──────────────────────────────────────────────
-- Batching protocol
-- ──────────────────────────────────────────────

-- | The result of probing a computation: either a final value
-- or a set of blocked fetches with a continuation.
data Status m f a
  = Done a
  | Blocked (Batches m) (f a)

instance Functor f => Functor (Status m f) where
  fmap f (Done a)       = Done (f a)
  fmap f (Blocked bs k) = Blocked bs (fmap f k)

-- | Transform the continuation type in a 'Status'.
mapStatus :: (forall x. f x -> g x) -> Status m f a -> Status m g a
mapStatus _ (Done a)       = Done a
mapStatus f (Blocked bs k) = Blocked bs (f k)

-- | A collection of pending fetch requests, grouped by key type.
newtype Batches m = Batches (Map SomeTypeRep (SomeBatch m))

instance Semigroup (Batches m) where
  Batches a <> Batches b = Batches (Map.unionWith mergeBatch a b)

instance Monoid (Batches m) where
  mempty = Batches Map.empty

-- | Total number of (deduplicated) keys across all sources.
batchSize :: Batches m -> Int
batchSize (Batches m) = Map.foldl' (\acc b -> acc + someBatchLen b) 0 m
  where
    someBatchLen :: SomeBatch n -> Int
    someBatchLen (SomeBatch ks) = HS.size ks

-- | Number of distinct data sources in this batch.
batchSourceCount :: Batches m -> Int
batchSourceCount (Batches m) = Map.size m

-- | Extract the keys for a specific source type (for testing/tracing).
batchKeys :: forall k m. (Typeable k) => Batches m -> [k]
batchKeys (Batches m) =
  case Map.lookup (someTypeRep (Proxy @k)) m of
    Just (SomeBatch (ks :: HashSet k')) ->
      case eqTypeRep (typeRep @k) (typeRep @k') of
        Just HRefl -> HS.toList ks
        Nothing    -> []
    Nothing -> []

-- | Existentially wraps a batch for a single data source.
-- The @m@ parameter carries the monad needed to dispatch.
data SomeBatch m = forall k.
  (DataSource m k, Typeable k, Typeable (Result k))
  => SomeBatch (HashSet k)

mergeBatch :: SomeBatch m -> SomeBatch m -> SomeBatch m
mergeBatch (SomeBatch (a :: HashSet k1)) (SomeBatch (b :: HashSet k2)) =
  case eqTypeRep (typeRep @k1) (typeRep @k2) of
    Just HRefl -> SomeBatch (HS.union a b)
    Nothing    -> error "Fetch.Class.mergeBatch: impossible type mismatch"

-- | Create a batch containing a single key.
singletonBatch :: forall m k.
  (DataSource m k, Typeable (Result k))
  => k -> Batches m
singletonBatch k = Batches $
  Map.singleton (someTypeRep (Proxy @k)) (SomeBatch (HS.singleton k))

-- ──────────────────────────────────────────────
-- MonadFetch
-- ──────────────────────────────────────────────

-- | The interface application code programs against.
--
-- @m@ is the /source monad/ (the monad that 'DataSource'
-- implementations run in). @n@ is the /fetch monad/: the monad
-- your application code runs in, typically 'Fetch' @m@.
--
-- The functional dependency @n -> m@ means the source monad is
-- determined by the fetch monad.
class Monad n => MonadFetch m n | n -> m where
  -- | Fetch a single key. Throws on error.
  fetch :: (DataSource m k, Typeable (Result k)) => k -> n (Result k)

  -- | Fetch a single key with explicit error handling.
  tryFetch :: (DataSource m k, Typeable (Result k))
           => k -> n (Either SomeException (Result k))

  -- | Seed the cache with a known key\/value pair.
  --
  -- * __Miss__: inserts a new resolved entry.
  -- * __Pending__: fills the in-flight IVar, waking any blocked
  --   continuations immediately. The batch\'s later write is
  --   idempotent and silently ignored.
  -- * __Resolved__: overwrites with the new value.
  --
  -- Use this to prime sub-entities extracted from compound responses:
  --
  -- @
  -- posts <- fetch (PostsFeed feedId)
  -- mapM_ (\\p -> primeCache (PostById (postId p)) p) posts
  -- @
  primeCache :: (FetchKey k, Typeable (Result k)) => k -> Result k -> n ()

-- | The batching protocol. Only needed by implementations and runners,
-- not by application code.
class MonadFetch m n => MonadFetchBatch m n | n -> m where
  -- | Expose the next blocking point of a computation.
  probe :: n a -> n (Status m n a)

  -- | Inject a 'Status' back into the monad.
  --
  -- __Warning:__ for a @'Blocked' batches k@ status, @embed@
  -- discards @batches@ and runs the continuation @k@ directly.
  -- The continuation expects its IVars to have been filled by the
  -- engine. If you call @embed@ on a 'Blocked' status without
  -- first executing the batches (via 'executeBatches' or the
  -- run-loop), every blocked key will error with
  -- @\"Key not found in cache after round\"@.
  --
  -- Typical usage: call 'probe' to inspect the status, execute
  -- the batches yourself, /then/ call @embed@ (or feed the
  -- continuation to the next iteration of your custom loop).
  embed :: Status m n a -> n a
  embed (Done a)      = pure a
  embed (Blocked _ k) = k

-- ──────────────────────────────────────────────
-- Mutations
-- ──────────────────────────────────────────────

-- | A typed key for a mutation operation. The key itself encodes
-- both the identity and the input (e.g., @UpdateUserName uid name@).
--
-- Unlike 'FetchKey', mutation keys only require 'Typeable' (no
-- 'Hashable', 'Eq', or 'Show') because mutations are never
-- deduplicated, cached, or shown in engine error messages.
class Typeable k => MutationKey k where
  type MutationResult k
