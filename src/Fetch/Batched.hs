{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE BangPatterns #-}

module Fetch.Batched
  ( Fetch(..)
  , FetchConfig(..)
  , fetchConfig
  , fetchConfigIO
  , FetchEnv(..)
  , liftSource
  , runFetch
  , runFetch'
  , runLoop
  , runLoopWith
  ) where

import Fetch.Class
import Fetch.Cache
import Fetch.IVar
import Fetch.Engine

import Control.Exception (throwIO, toException)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import Control.Monad.IO.Unlift (MonadUnliftIO(..), liftIO)
import Data.Functor.Apply (Apply(..))
import Data.Functor.Bind (Bind(..))
import qualified Data.HashMap.Strict as HM

-- | The environment threaded through Fetch.
--
-- Contains the cache and the two natural transformations that
-- bridge the source monad @m@ with @IO@.
data FetchEnv m = FetchEnv
  { fetchCache :: !CacheRef
  , fetchLower :: !(forall x. m x -> IO x)
    -- ^ Run an @m@ action in @IO@. Used by the engine to
    -- dispatch @batchFetch@ calls.
  , fetchLift  :: !(forall x. IO x -> m x)
    -- ^ Lift an @IO@ action into @m@. Used for cache operations
    -- and IVar interactions within @m@.
  }

-- | The core monad transformer. Supports Applicative batching:
-- independent fetches in @\<*\>@ merge into a single round,
-- while @>>=@ introduces a round boundary.
--
-- The @m@ parameter is the /source monad/: the monad that
-- 'DataSource' implementations run in.
--
-- Enable @ApplicativeDo@ for ergonomic batching in do-blocks.
newtype Fetch m a = Fetch
  { unFetch :: FetchEnv m -> m (Status m (Fetch m) a) }

-- | Lift a source-monad action into 'Fetch'.
liftSource :: Monad m => m a -> Fetch m a
liftSource ma = Fetch $ \_ -> Done <$> ma

instance Functor m => Functor (Fetch m) where
  fmap f (Fetch g) = Fetch $ \e -> fmap (fmap f) (g e)

instance Monad m => Applicative (Fetch m) where
  pure a = Fetch $ \_ -> pure (Done a)

  Fetch ff <*> Fetch fx = Fetch $ \e -> do
    sf <- ff e
    sx <- fx e
    pure $ case (sf, sx) of
      (Done f, Done x) ->
        Done (f x)
      (Done f, Blocked bs kx) ->
        Blocked bs (fmap f kx)
      (Blocked bs kf, Done x) ->
        Blocked bs (fmap ($ x) kf)
      (Blocked bs1 kf, Blocked bs2 kx) ->
        Blocked (bs1 <> bs2) (kf <*> kx)

instance Monad m => Monad (Fetch m) where
  Fetch ma >>= f = Fetch $ \e -> do
    sa <- ma e
    case sa of
      Done a       -> unFetch (f a) e
      Blocked bs k -> pure $ Blocked bs (k >>= f)

instance MonadFail m => MonadFail (Fetch m) where
  fail = liftSource . fail

-- ──────────────────────────────────────────────
-- MonadThrow / MonadCatch
-- ──────────────────────────────────────────────

instance MonadThrow m => MonadThrow (Fetch m) where
  throwM = liftSource . throwM

-- | Propagates the handler through 'Blocked' continuations so that
-- a @catch@ wrapping a multi-round computation catches exceptions
-- thrown in any round, not just the initial probe.
instance MonadCatch m => MonadCatch (Fetch m) where
  catch (Fetch f) handler = Fetch $ \e -> do
    status <- catch (f e) (\ex -> unFetch (handler ex) e)
    case status of
      Done a       -> pure (Done a)
      Blocked bs k -> pure (Blocked bs (catch k handler))

-- ──────────────────────────────────────────────
-- Semigroup / Monoid (lifted)
-- ──────────────────────────────────────────────

-- | Combines two fetches applicatively, batching their pending keys.
--
-- @a <> b = liftA2 (<>) a b@
instance (Monad m, Semigroup a) => Semigroup (Fetch m a) where
  (<>) = liftA2 (<>)

-- | @mempty = pure mempty@.
instance (Monad m, Monoid a) => Monoid (Fetch m a) where
  mempty = pure mempty

-- ──────────────────────────────────────────────
-- Semigroupoids (Apply / Bind)
-- ──────────────────────────────────────────────

-- | 'Apply' is 'Applicative' without 'pure'. Same batching semantics.
instance Monad m => Apply (Fetch m) where
  (<.>) = (<*>)

-- | 'Bind' is 'Monad' without 'return'. Same round-boundary semantics.
instance Monad m => Bind (Fetch m) where
  (>>-) = (>>=)

-- ──────────────────────────────────────────────
-- Instances that are NOT provided (and why)
-- ──────────────────────────────────────────────

-- MonadTrans / MonadIO:
--   Intentionally omitted. @lift@ / @liftIO@ would be equivalent to
--   'liftSource', but having them available via the standard typeclasses
--   makes it too easy to accidentally run source-monad actions during
--   the probe phase — bypassing the batching system and potentially
--   introducing writes outside of 'Mutate'\'s cache reconciliation.
--   Use 'liftSource' when you explicitly need to lift an @m@ action.
--
-- MFunctor / hoist (mmorph):
--   NOT possible. 'Batches m' carries existential @DataSource m k@
--   constraints. Changing @m@ to @n@ requires re-proving those
--   constraints for @n@, which cannot be done generically.
--
-- MonadReader r:
--   'ask' would work via @lift ask@, but 'local' cannot propagate
--   through batch dispatch. The 'fetchLower' nat in 'FetchEnv'
--   captures the reader environment at the run site; 'local' inside
--   a 'Fetch' computation would only affect the probe phase, not
--   the 'batchFetch' calls dispatched by the engine. Providing a
--   'MonadReader' instance with broken 'local' would violate the
--   class laws, so we omit it entirely.
--
-- MonadBaseControl / MonadUnliftIO:
--   NOT possible. 'Fetch' is continuation-based: a 'Blocked' status
--   carries thunks that close over the 'FetchEnv' (which contains
--   mutable 'CacheRef' state). There is no way to capture this as a
--   pure @StM@ value and restore it.
--
-- MonadMask:
--   Intentionally omitted. Async exception masking across batch
--   round boundaries is not well-defined. A @mask@ would need to
--   protect both the probe and all subsequent rounds, but rounds
--   execute in IO via 'executeBatches' which uses 'async' internally.

-- ──────────────────────────────────────────────
-- Cache lookup helper
-- ──────────────────────────────────────────────

-- | Look up a key in the cache, awaiting any pending IVar.
-- Returns @Right v@ on hit, @Left ex@ on error/miss.
-- The @onMiss@ fallback is invoked (in @m@) when the key is absent.
lookupOrAwait :: (FetchKey k, Typeable (Result k), Monad m)
              => FetchEnv m -> k -> m (Either SomeException (Result k))
              -> m (Either SomeException (Result k))
lookupOrAwait e k onMiss = do
  hit <- fetchLift e $ cacheLookup (fetchCache e) k
  case hit of
    CacheHitReady v    -> pure (Right v)
    CacheHitPending iv -> fetchLift e $ awaitIVar iv
    CacheMiss          -> onMiss

-- ──────────────────────────────────────────────
-- MonadFetch instance
-- ──────────────────────────────────────────────

instance Monad m => MonadFetch m (Fetch m) where
  fetch (k :: k) = Fetch $ \e ->
    -- NoCaching semantics: skip the cache check entirely and always
    -- return Blocked. This guarantees that every >>= round dispatches
    -- a fresh batch for this key. The engine's dispatchUncached uses
    -- cacheAllocateForce to overwrite any stale IVar from a prior
    -- round, so the continuation below always reads a fresh result.
    --
    -- Within a single applicative round, dedup still works: the
    -- HashSet in SomeBatch merges duplicate keys, and all
    -- continuations from the same round share the one fresh IVar
    -- via lookupOrAwait.
    case cachePolicy @m @k Proxy of
      NoCaching ->
        pure $ Blocked
          (singletonBatch k)
          (Fetch $ \e' -> do
            result <- lookupOrAwait e' k
              (fetchLift e' $ throwIO $ FetchError $
                "Key not found in cache after round: " <> show k)
            case result of
              Right v -> pure (Done v)
              Left ex -> fetchLift e' $ throwIO ex)
      CacheResults -> do
        -- Check cache first
        hit <- fetchLift e $ cacheLookup (fetchCache e) k
        case hit of
          CacheHitReady v  -> pure (Done v)
          CacheHitPending iv -> do
            result <- fetchLift e $ awaitIVar iv
            case result of
              Right v -> pure (Done v)
              Left ex -> fetchLift e $ throwIO ex
          CacheMiss ->
            pure $ Blocked
              (singletonBatch k)
              (Fetch $ \e' -> do
                result <- lookupOrAwait e' k
                  (fetchLift e' $ throwIO $ FetchError $
                    "Key not found in cache after round: " <> show k)
                case result of
                  Right v -> pure (Done v)
                  Left ex -> fetchLift e' $ throwIO ex)

  tryFetch (k :: k) = Fetch $ \e ->
    -- See 'fetch' above for NoCaching semantics.
    case cachePolicy @m @k Proxy of
      NoCaching ->
        pure $ Blocked
          (singletonBatch k)
          (Fetch $ \e' -> do
            result <- lookupOrAwait e' k
              (pure $ Left $ toException $
                FetchError $ "Key not found in cache after round: " <> show k)
            pure (Done result))
      CacheResults -> do
        hit <- fetchLift e $ cacheLookup (fetchCache e) k
        case hit of
          CacheHitReady v  -> pure (Done (Right v))
          CacheHitPending iv -> do
            result <- fetchLift e $ awaitIVar iv
            pure (Done result)
          CacheMiss ->
            pure $ Blocked
              (singletonBatch k)
              (Fetch $ \e' -> do
                result <- lookupOrAwait e' k
                  (pure $ Left $ toException $
                    FetchError $ "Key not found in cache after round: " <> show k)
                pure (Done result))

  primeCache k v = Fetch $ \e -> do
    let cRef = fetchCache e
    hit <- fetchLift e $ cacheLookup cRef k
    case hit of
      CacheHitPending iv -> fetchLift e $ writeIVar iv v
      _                  -> fetchLift e $ cacheWarm cRef (HM.singleton k v)
    pure (Done ())

-- ──────────────────────────────────────────────
-- MonadFetchBatch instance
-- ──────────────────────────────────────────────

instance Monad m => MonadFetchBatch m (Fetch m) where
  probe m = Fetch $ \e -> do
    s <- unFetch m e
    pure (Done s)

  embed (Done a)      = pure a
  embed (Blocked _ k) = k

-- ──────────────────────────────────────────────
-- Config
-- ──────────────────────────────────────────────

-- | Configuration for running a 'Fetch' computation.
--
-- Contains the two natural transformations that bridge the source
-- monad @m@ with @IO@, plus optional settings. Use 'fetchConfig'
-- to construct with sensible defaults, then override fields as needed:
--
-- @
-- let cfg = fetchConfig (runAppM env) liftIO
-- runFetch cfg action
--
-- -- with a shared cache:
-- runFetch cfg { configCache = Just myCache } action
-- @
--
-- For monads that deliberately avoid 'MonadIO' (e.g. a @Transaction@
-- type), the nats are the private escape hatches:
--
-- @
-- fetchInTransaction :: Fetch Transaction a -> Transaction a
-- fetchInTransaction = runFetch (fetchConfig unsafeRunTransaction unsafeLiftIO)
-- @
data FetchConfig m = FetchConfig
  { configLower :: !(forall x. m x -> IO x)
    -- ^ Run an @m@ action in @IO@. Used by the engine to dispatch
    -- @batchFetch@ calls.
  , configLift  :: !(forall x. IO x -> m x)
    -- ^ Lift an @IO@ action into @m@. Used for cache and IVar
    -- operations within @m@.
  , configCache :: !(Maybe CacheRef)
    -- ^ Pre-existing cache. 'Nothing' creates a fresh cache per run.
    -- Set to @Just cRef@ to share or pre-warm a cache.
  }

-- | Construct a 'FetchConfig' with explicit natural transformations.
--
-- Use this for monads that don't have 'MonadUnliftIO' (e.g. a
-- restricted @Transaction@ type). For 'MonadUnliftIO' monads,
-- prefer 'fetchConfigIO' which fills in the nats automatically.
fetchConfig :: (forall x. m x -> IO x)
            -> (forall x. IO x -> m x)
            -> FetchConfig m
fetchConfig lower lift = FetchConfig
  { configLower = lower
  , configLift  = lift
  , configCache = Nothing
  }

-- | Construct a 'FetchConfig' for any 'MonadUnliftIO' monad.
--
-- The natural transformations are derived from the 'MonadUnliftIO'
-- instance: 'withRunInIO' provides @m x -> IO x@, and 'liftIO'
-- provides @IO x -> m x@.
--
-- @
-- cfg <- fetchConfigIO
-- runFetch cfg action
-- @
fetchConfigIO :: MonadUnliftIO m => m (FetchConfig m)
fetchConfigIO = withRunInIO $ \runInIO ->
  pure FetchConfig
    { configLower = runInIO
    , configLift  = liftIO
    , configCache = Nothing
    }

-- ──────────────────────────────────────────────
-- Runners
-- ──────────────────────────────────────────────

-- | Run a 'Fetch' computation.
--
-- @
-- let cfg = fetchConfig (runAppM env) liftIO
-- result <- runFetch cfg action
-- @
--
-- To share a cache across sequential phases:
--
-- @
-- let cfg = (fetchConfig lower lift) { configCache = Just myCache }
-- runFetch cfg action
-- @
runFetch :: Monad m => FetchConfig m -> Fetch m a -> m a
runFetch cfg action = do
  cacheRef <- case configCache cfg of
    Just ref -> pure ref
    Nothing  -> configLift cfg newCacheRef
  let e = FetchEnv
        { fetchCache = cacheRef
        , fetchLower = configLower cfg
        , fetchLift  = configLift cfg
        }
  runLoop e (\_ _ -> pure ()) action

-- | Like 'runFetch', but also returns the 'CacheRef'.
-- This is the @runStateT@-style variant for cache preservation:
--
-- @
-- (result1, cache) <- runFetch' cfg phase1
-- result2 <- runFetch cfg { configCache = Just cache } phase2
-- @
runFetch' :: Monad m => FetchConfig m -> Fetch m a -> m (a, CacheRef)
runFetch' cfg action = do
  cacheRef <- case configCache cfg of
    Just ref -> pure ref
    Nothing  -> configLift cfg newCacheRef
  let e = FetchEnv
        { fetchCache = cacheRef
        , fetchLower = configLower cfg
        , fetchLift  = configLift cfg
        }
  a <- runLoop e (\_ _ -> pure ()) action
  pure (a, cacheRef)

-- | Generalised execution loop with a per-round wrapper.
--
-- The callback receives:
--
-- * The 1-based round number.
-- * The pending 'Batches'.
-- * An action that executes the batches and returns 'RoundStats'.
--
-- The callback /must/ invoke the execution action for the computation
-- to make progress. This design lets instrumentation wrap /around/
-- batch execution (opening a tracing span before and closing it
-- after, for example).
--
-- @
-- runLoopWith env (\\n batches exec -> do
--     openSpan n batches
--     stats <- exec
--     closeSpan stats) action
-- @
runLoopWith :: Monad m
            => FetchEnv m
            -> (Int -> Batches m -> m RoundStats -> m ())
               -- ^ Round wrapper. Must invoke the @m RoundStats@ action.
            -> Fetch m a
            -> m a
runLoopWith e withRound = go 1
  where
    go !n m = do
      status <- unFetch m e
      case status of
        Done a -> pure a
        Blocked batches k -> do
          let exec = fetchLift e $
                executeBatches (fetchLower e) (fetchLift e) (fetchCache e) batches
          withRound n batches exec
          go (n + 1) k

-- | Simplified execution loop with a pre-round callback.
--
-- Equivalent to 'runLoopWith' where the callback fires before batch
-- execution but does not wrap it.
--
-- Exposed for use by custom runners.
runLoop :: Monad m
        => FetchEnv m
        -> (Int -> Batches m -> m ())
           -- ^ Called before each round with round number and batches.
        -> Fetch m a
        -> m a
runLoop e onRound = runLoopWith e $ \n batches exec -> do
  onRound n batches
  _ <- exec
  pure ()
