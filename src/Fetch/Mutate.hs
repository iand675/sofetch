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
-- 'MutateT' is a free-monad-like transformer layered on top of 'FetchT'.
-- A computation is a sequence of __fetch phases__ (batched reads via
-- 'FetchT') interleaved with __mutation steps__ (sequential writes).
--
-- Mutations are inert data during 'FetchT' probing; they only
-- execute when the runner processes them. This guarantees:
--
-- * Within a fetch phase: all fetches batch via 'FetchT'\'s 'Applicative'.
-- * Between phases: mutations execute one at a time, sequentially.
-- * In @\<*\>@: fetches run first (batched), then mutations left-to-right.
-- * Cache consistency: 'reconcileCache' runs after each mutation,
--   before any subsequent fetch phase sees the cache.
module Fetch.Mutate
  ( -- * Mutation classes
    MutationSource(..)
  , MonadMutate(..)
    -- * MutateT transformer
  , MutateT(..)
  , Step(..)
  , liftFetchT
    -- * Runners
  , runMutateT
  , runMutateTWithCache
  ) where

import Fetch.Class
import Fetch.Cache (CacheRef, newCacheRef)
import Fetch.Batched (FetchT, runFetchTWithCache)

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
  -- 'FetchT' probing.
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
    => StepMutate k (MutationResult k -> MutateT m n a)
  | forall k. MutationSource m k
    => StepTryMutate k (Either SomeException (MutationResult k) -> MutateT m n a)

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

-- | Inject a 'Step' into 'MutateT' as a trivial fetch phase.
embedStep :: Monad n => Step m n a -> MutateT m n a
embedStep (StepDone a)          = MutateT (pure (StepDone a))
embedStep (StepMutate k cont)   = MutateT (pure (StepMutate k cont))
embedStep (StepTryMutate k cont) = MutateT (pure (StepTryMutate k cont))

-- ──────────────────────────────────────────────
-- MutateT
-- ──────────────────────────────────────────────

-- | A computation that interleaves batched fetch phases with
-- sequential mutations.
--
-- @m@ is the source monad (same as in 'DataSource' and 'FetchT').
-- @n@ is the base monad for 'FetchT'.
--
-- In practice, @n@ is always @m@ and 'MutateT m m' is layered on
-- 'FetchT m'.
--
-- @
-- do (user, posts) <- (,) \<$\> fetch uid \<*\> fetch pid  -- batched
--    updated <- mutate (UpdateUserName uid "new")        -- mutation boundary
--    fetch uid                                            -- cache hit
-- @
newtype MutateT m n a = MutateT
  { unMutateT :: FetchT n (Step m n a) }

instance Monad n => Functor (MutateT m n) where
  fmap f (MutateT inner) = MutateT (fmap (mapStep f) inner)

instance Monad n => Applicative (MutateT m n) where
  pure a = MutateT (pure (StepDone a))

  MutateT ff <*> MutateT fx = MutateT $
    -- Delegate to FetchT's Applicative for batching, then combine Steps.
    liftA2 apStep ff fx

instance Monad n => Monad (MutateT m n) where
  MutateT ma >>= f = MutateT $ do
    step <- ma  -- runs in FetchT
    case step of
      StepDone a -> unMutateT (f a)  -- continue in same FetchT phase
      StepMutate k cont ->
        pure $ StepMutate k (\r -> cont r >>= f)
      StepTryMutate k cont ->
        pure $ StepTryMutate k (\r -> cont r >>= f)

-- ──────────────────────────────────────────────
-- Instances
-- ──────────────────────────────────────────────

instance Monad m => MonadFetch m (MutateT m m) where
  fetch k      = MutateT (StepDone <$> fetch k)
  tryFetch k   = MutateT (StepDone <$> tryFetch k)
  primeCache k v = MutateT (StepDone <$> primeCache k v)

instance Monad m => MonadMutate m (MutateT m m) where
  mutate k    = MutateT (pure (StepMutate k pure))
  tryMutate k = MutateT (pure (StepTryMutate k pure))

instance MonadFail m => MonadFail (MutateT m m) where
  fail msg = MutateT (fail msg)

instance MonadThrow m => MonadThrow (MutateT m m) where
  throwM e = MutateT (throwM e)

-- | Propagates the handler through both 'FetchT' round continuations
-- (handled by 'FetchT'\'s 'MonadCatch') and 'Step' mutation
-- continuations.
instance MonadCatch m => MonadCatch (MutateT m m) where
  catch (MutateT inner) handler = MutateT $
    fmap catchStep $
      catch inner (\ex -> unMutateT (handler ex))
    where
      catchStep (StepDone a)           = StepDone a
      catchStep (StepMutate k cont)    = StepMutate k (\r -> catch (cont r) handler)
      catchStep (StepTryMutate k cont) = StepTryMutate k (\r -> catch (cont r) handler)

-- | Lift a 'FetchT' computation into 'MutateT'.
-- The entire 'FetchT' runs as a single fetch phase.
liftFetchT :: Monad m => FetchT m a -> MutateT m m a
liftFetchT action = MutateT (StepDone <$> action)

-- ──────────────────────────────────────────────
-- Runners
-- ──────────────────────────────────────────────

-- | Run a 'MutateT' computation with a fresh cache.
runMutateT :: Monad m
           => (forall x. m x -> IO x)
           -> (forall x. IO x -> m x)
           -> MutateT m m a
           -> m a
runMutateT lower lift action = do
  cRef <- lift newCacheRef
  runMutateTWithCache lower lift cRef action

-- | Run a 'MutateT' computation with an externally-provided cache.
-- Useful for sharing cache across runs or pre-warming.
runMutateTWithCache :: forall m a. Monad m
                    => (forall x. m x -> IO x)
                    -> (forall x. IO x -> m x)
                    -> CacheRef
                    -> MutateT m m a
                    -> m a
runMutateTWithCache lower lift cRef = go
  where
    go :: MutateT m m a -> m a
    go (MutateT fetchPhase) = do
      step <- runFetchTWithCache lower lift cRef fetchPhase
      case step of
        StepDone a -> pure a
        StepMutate k cont -> do
          result <- executeMutation k
          lift $ reconcileCache @m k result cRef
          go (cont result)
        StepTryMutate k cont -> do
          result <- lift $ try $ lower $ executeMutation k
          case result of
            Right v -> do
              lift $ reconcileCache @m k v cRef
              go (cont (Right v))
            Left ex ->
              go (cont (Left ex))
