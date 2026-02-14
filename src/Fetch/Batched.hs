{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE BangPatterns #-}

module Fetch.Batched
  ( FetchT(..)
  , FetchEnv(..)
  , liftSource
  , runFetchT
  , runFetchTWithCache
  , runLoop
  , runLoopWith
  ) where

import Fetch.Class
import Fetch.Cache
import Fetch.IVar
import Fetch.Engine

import Control.Exception (throwIO, toException)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import qualified Data.HashMap.Strict as HM

-- | The environment threaded through FetchT.
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
newtype FetchT m a = FetchT
  { unFetchT :: FetchEnv m -> m (Status m (FetchT m) a) }

-- | Lift a source-monad action into 'FetchT'.
liftSource :: Monad m => m a -> FetchT m a
liftSource ma = FetchT $ \_ -> Done <$> ma

instance Functor m => Functor (FetchT m) where
  fmap f (FetchT g) = FetchT $ \e -> fmap (fmap f) (g e)

instance Monad m => Applicative (FetchT m) where
  pure a = FetchT $ \_ -> pure (Done a)

  FetchT ff <*> FetchT fx = FetchT $ \e -> do
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

instance Monad m => Monad (FetchT m) where
  FetchT ma >>= f = FetchT $ \e -> do
    sa <- ma e
    case sa of
      Done a       -> unFetchT (f a) e
      Blocked bs k -> pure $ Blocked bs (k >>= f)

instance MonadFail m => MonadFail (FetchT m) where
  fail = liftSource . fail

-- ──────────────────────────────────────────────
-- MonadThrow / MonadCatch
-- ──────────────────────────────────────────────

instance MonadThrow m => MonadThrow (FetchT m) where
  throwM = liftSource . throwM

-- | Propagates the handler through 'Blocked' continuations so that
-- a @catch@ wrapping a multi-round computation catches exceptions
-- thrown in any round, not just the initial probe.
instance MonadCatch m => MonadCatch (FetchT m) where
  catch (FetchT f) handler = FetchT $ \e -> do
    status <- catch (f e) (\ex -> unFetchT (handler ex) e)
    case status of
      Done a       -> pure (Done a)
      Blocked bs k -> pure (Blocked bs (catch k handler))

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

instance Monad m => MonadFetch m (FetchT m) where
  fetch (k :: k) = FetchT $ \e ->
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
          (FetchT $ \e' -> do
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
              (FetchT $ \e' -> do
                result <- lookupOrAwait e' k
                  (fetchLift e' $ throwIO $ FetchError $
                    "Key not found in cache after round: " <> show k)
                case result of
                  Right v -> pure (Done v)
                  Left ex -> fetchLift e' $ throwIO ex)

  tryFetch (k :: k) = FetchT $ \e ->
    -- See 'fetch' above for NoCaching semantics.
    case cachePolicy @m @k Proxy of
      NoCaching ->
        pure $ Blocked
          (singletonBatch k)
          (FetchT $ \e' -> do
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
              (FetchT $ \e' -> do
                result <- lookupOrAwait e' k
                  (pure $ Left $ toException $
                    FetchError $ "Key not found in cache after round: " <> show k)
                pure (Done result))

  primeCache k v = FetchT $ \e -> do
    let cRef = fetchCache e
    hit <- fetchLift e $ cacheLookup cRef k
    case hit of
      CacheHitPending iv -> fetchLift e $ writeIVar iv v
      _                  -> fetchLift e $ cacheWarm cRef (HM.singleton k v)
    pure (Done ())

-- ──────────────────────────────────────────────
-- MonadFetchBatch instance
-- ──────────────────────────────────────────────

instance Monad m => MonadFetchBatch m (FetchT m) where
  probe m = FetchT $ \e -> do
    s <- unFetchT m e
    pure (Done s)

  embed (Done a)      = pure a
  embed (Blocked _ k) = k

-- ──────────────────────────────────────────────
-- Runners
-- ──────────────────────────────────────────────

-- | Run a 'FetchT' computation.
--
-- Takes two natural transformations:
--
-- * @lower@: @m x -> IO x@. Runs source-monad actions in IO.
-- * @lift@: @IO x -> m x@. Lifts IO actions into the source monad.
--
-- Returns in the source monad @m@.
--
-- For libraries that use a restricted monad (e.g. @Transaction@)
-- without @MonadIO@ / @MonadUnliftIO@, these nats are the escape
-- hatch:
--
-- @
-- fetchInTransaction :: FetchT Transaction a -> Transaction a
-- fetchInTransaction = runFetchT unsafeRunTransaction unsafeLiftIO
-- @
runFetchT :: Monad m
          => (forall x. m x -> IO x)
          -> (forall x. IO x -> m x)
          -> FetchT m a
          -> m a
runFetchT lower lift action = do
  cacheRef <- lift newCacheRef
  runFetchTWithCache lower lift cacheRef action

-- | Run with an externally-provided cache. Useful for:
--
-- * Sharing cache across sequential phases of request processing
-- * Pre-warming the cache before running
-- * Inspecting cache contents after running
runFetchTWithCache :: Monad m
                   => (forall x. m x -> IO x)
                   -> (forall x. IO x -> m x)
                   -> CacheRef
                   -> FetchT m a
                   -> m a
runFetchTWithCache lower lift cacheRef action = do
  let e = FetchEnv
        { fetchCache = cacheRef
        , fetchLower = lower
        , fetchLift  = lift
        }
  runLoop e (\_ _ -> pure ()) action

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
            -> FetchT m a
            -> m a
runLoopWith e withRound = go 1
  where
    go !n m = do
      status <- unFetchT m e
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
        -> FetchT m a
        -> m a
runLoop e onRound = runLoopWith e $ \n batches exec -> do
  onRound n batches
  _ <- exec
  pure ()
