{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | Principled mutation support for sofetch.
--
-- 'Mutate' is a free-monad-like transformer layered on top of 'Fetch'.
-- A computation is a sequence of __fetch phases__ (batched reads via
-- 'Fetch') interleaved with __mutation steps__ (sequential writes).
--
-- Mutations are inert data during 'Fetch' probing; they only
-- execute when the runner processes them. This guarantees:
--
-- * Within a fetch phase: all fetches batch via 'Fetch'\'s 'Applicative'.
-- * Between phases: mutations execute one at a time, sequentially.
-- * In @\<*\>@: fetches run first (batched), then mutations left-to-right.
-- * Cache consistency: 'reconcileCache' runs after each mutation,
--   before any subsequent fetch phase sees the cache.
module Fetch.Mutate
  ( -- * Mutation classes
    MutationSource(..)
  , MonadMutate(..)
    -- * Mutate transformer
  , Mutate(..)
  , Step(..)
  , liftFetch
    -- * Runners
  , runMutate
  ) where

import Fetch.Class
import Fetch.Cache (CacheRef, newCacheRef)
import Fetch.Batched (Fetch, FetchConfig(..), runFetch)

import Control.Exception (try)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))

-- ──────────────────────────────────────────────
-- MutationSource
-- ──────────────────────────────────────────────

-- | How to execute a mutation in the source monad @m@.
--
-- The @m@ parameter replaces the old @env@ parameter, just as
-- in 'DataSource'. The monad @m@ provides access to any needed
-- resources (database connections, etc.).
--
-- @
-- instance MutationSource AppM UpdateUserName where
--   executeMutation (UpdateUserName uid name) =
--     updateUserInDB uid name
--
--   reconcileCache (UpdateUserName uid _) result cRef =
--     cacheWarm cRef (HM.singleton (UserId uid) result)
-- @
class (MutationKey k, Typeable (MutationResult k)) => MutationSource m k where
  -- | Execute the mutation. Called by the runner, never during
  -- 'Fetch' probing.
  executeMutation :: k -> m (MutationResult k)

  -- | Reconcile the cache after a successful mutation.
  -- Use this to evict stale entries or warm the cache with
  -- fresh data from the mutation response.
  --
  -- Note: 'reconcileCache' runs in @IO@ because cache operations
  -- are inherently @IO@-based ('CacheRef' is an 'IORef').
  --
  -- Default: no-op.
  reconcileCache :: k -> MutationResult k -> CacheRef -> IO ()
  reconcileCache _ _ _ = pure ()

-- ──────────────────────────────────────────────
-- MonadMutate
-- ──────────────────────────────────────────────

-- | The mutation interface. Extends 'MonadFetch' with write operations.
--
-- @mutate@ ends the current fetch phase, executes the mutation via
-- the runner, reconciles the cache, and returns the result. Subsequent
-- fetches see the reconciled cache.
class MonadFetch m n => MonadMutate m n | n -> m where
  -- | Execute a mutation. Throws on error.
  mutate :: MutationSource m k => k -> n (MutationResult k)

  -- | Execute a mutation with explicit error handling.
  tryMutate :: MutationSource m k
            => k -> n (Either SomeException (MutationResult k))

-- ──────────────────────────────────────────────
-- Step
-- ──────────────────────────────────────────────

-- | The result of a fetch phase: either a final value or a mutation
-- boundary with a continuation.
data Step m n a
  = StepDone a
  | forall k. MutationSource m k
    => StepMutate k (MutationResult k -> Mutate m n a)
  | forall k. MutationSource m k
    => StepTryMutate k (Either SomeException (MutationResult k) -> Mutate m n a)

-- | Map a function over the final value of a 'Step'.
mapStep :: Monad n => (a -> b) -> Step m n a -> Step m n b
mapStep f (StepDone a)          = StepDone (f a)
mapStep f (StepMutate k cont)   = StepMutate k (fmap f . cont)
mapStep f (StepTryMutate k cont) = StepTryMutate k (fmap f . cont)

-- | Combine two 'Step' values applicatively.
-- Fetch-phase results combine directly; mutations sequence left-to-right.
apStep :: Monad n => Step m n (a -> b) -> Step m n a -> Step m n b
apStep (StepDone f) (StepDone x) =
  StepDone (f x)
apStep (StepDone f) (StepMutate k cont) =
  StepMutate k (fmap f . cont)
apStep (StepDone f) (StepTryMutate k cont) =
  StepTryMutate k (fmap f . cont)
apStep (StepMutate k cont) (StepDone x) =
  StepMutate k (fmap ($ x) . cont)
apStep (StepTryMutate k cont) (StepDone x) =
  StepTryMutate k (fmap ($ x) . cont)
-- Two mutations: sequence left first, then embed the right step
-- into the left's continuation.
apStep (StepMutate k1 cont1) step2 =
  StepMutate k1 $ \r1 ->
    cont1 r1 <*> embedStep step2
apStep (StepTryMutate k1 cont1) step2 =
  StepTryMutate k1 $ \r1 ->
    cont1 r1 <*> embedStep step2

-- | Inject a 'Step' into 'Mutate' as a trivial fetch phase.
embedStep :: Monad n => Step m n a -> Mutate m n a
embedStep (StepDone a)          = Mutate (pure (StepDone a))
embedStep (StepMutate k cont)   = Mutate (pure (StepMutate k cont))
embedStep (StepTryMutate k cont) = Mutate (pure (StepTryMutate k cont))

-- ──────────────────────────────────────────────
-- Mutate
-- ──────────────────────────────────────────────

-- | A computation that interleaves batched fetch phases with
-- sequential mutations.
--
-- @m@ is the source monad (same as in 'DataSource' and 'Fetch').
-- @n@ is the base monad for 'Fetch'.
--
-- In practice, @n@ is always @m@ and 'Mutate m m' is layered on
-- 'Fetch m'.
--
-- @
-- do (user, posts) <- (,) \<$\> fetch uid \<*\> fetch pid  -- batched
--    updated <- mutate (UpdateUserName uid "new")        -- mutation boundary
--    fetch uid                                            -- cache hit
-- @
newtype Mutate m n a = Mutate
  { unMutate :: Fetch n (Step m n a) }

instance Monad n => Functor (Mutate m n) where
  fmap f (Mutate inner) = Mutate (fmap (mapStep f) inner)

instance Monad n => Applicative (Mutate m n) where
  pure a = Mutate (pure (StepDone a))

  Mutate ff <*> Mutate fx = Mutate $
    -- Delegate to Fetch's Applicative for batching, then combine Steps.
    liftA2 apStep ff fx

instance Monad n => Monad (Mutate m n) where
  Mutate ma >>= f = Mutate $ do
    step <- ma  -- runs in Fetch
    case step of
      StepDone a -> unMutate (f a)  -- continue in same Fetch phase
      StepMutate k cont ->
        pure $ StepMutate k (\r -> cont r >>= f)
      StepTryMutate k cont ->
        pure $ StepTryMutate k (\r -> cont r >>= f)

-- ──────────────────────────────────────────────
-- Instances
-- ──────────────────────────────────────────────

instance Monad m => MonadFetch m (Mutate m m) where
  fetch k      = Mutate (StepDone <$> fetch k)
  tryFetch k   = Mutate (StepDone <$> tryFetch k)
  primeCache k v = Mutate (StepDone <$> primeCache k v)

instance Monad m => MonadMutate m (Mutate m m) where
  mutate k    = Mutate (pure (StepMutate k pure))
  tryMutate k = Mutate (pure (StepTryMutate k pure))

instance MonadFail m => MonadFail (Mutate m m) where
  fail msg = Mutate (fail msg)

instance MonadThrow m => MonadThrow (Mutate m m) where
  throwM e = Mutate (throwM e)

-- | Propagates the handler through both 'Fetch' round continuations
-- (handled by 'Fetch'\'s 'MonadCatch') and 'Step' mutation
-- continuations.
instance MonadCatch m => MonadCatch (Mutate m m) where
  catch (Mutate inner) handler = Mutate $
    fmap catchStep $
      catch inner (\ex -> unMutate (handler ex))
    where
      catchStep (StepDone a)           = StepDone a
      catchStep (StepMutate k cont)    = StepMutate k (\r -> catch (cont r) handler)
      catchStep (StepTryMutate k cont) = StepTryMutate k (\r -> catch (cont r) handler)

-- | Lift a 'Fetch' computation into 'Mutate'.
-- The entire 'Fetch' runs as a single fetch phase.
liftFetch :: Monad m => Fetch m a -> Mutate m m a
liftFetch action = Mutate (StepDone <$> action)

-- ──────────────────────────────────────────────
-- Runners
-- ──────────────────────────────────────────────

-- | Run a 'Mutate' computation.
--
-- @
-- let cfg = fetchConfig (runAppM env) liftIO
-- runMutate cfg action
-- @
runMutate :: forall m a. Monad m => FetchConfig m -> Mutate m m a -> m a
runMutate cfg action = do
  cRef <- case configCache cfg of
    Just ref -> pure ref
    Nothing  -> configLift cfg newCacheRef
  let fetchCfg = cfg { configCache = Just cRef }
      go :: Mutate m m a -> m a
      go (Mutate fetchPhase) = do
        step <- runFetch fetchCfg fetchPhase
        case step of
          StepDone a -> pure a
          StepMutate k cont -> do
            result <- executeMutation k
            configLift cfg $ reconcileCache @m k result cRef
            go (cont result)
          StepTryMutate k cont -> do
            result <- configLift cfg $ try $ configLower cfg $ executeMutation k
            case result of
              Right v -> do
                configLift cfg $ reconcileCache @m k v cRef
                go (cont (Right v))
              Left ex ->
                go (cont (Left ex))
  go action
