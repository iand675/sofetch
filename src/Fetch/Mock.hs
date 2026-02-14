{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE KindSignatures #-}

module Fetch.Mock
  ( -- * Mock fetch
    MockFetchT
  , runMockFetchT
  , ResultMap
  , mockData
  , emptyMockData
    -- * Mock mutations
  , MockMutateT
  , runMockMutateT
  , MutationHandlers
  , mockMutation
  , emptyMutationHandlers
  , RecordedMutation(..)
  ) where

import Data.Kind (Type)

import Fetch.Class
import Fetch.Mutate (MonadMutate(..))
import Fetch.IVar (FetchError(..))

import Control.Exception (toException, try, evaluate, throw, throwIO)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import Data.Dynamic
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Type.Reflection (SomeTypeRep(..), someTypeRep, typeOf)

-- | Pre-built result data for testing. Each entry wraps a
-- @HashMap k (Result k)@ as a Dynamic, keyed by source type.
newtype ResultMap = ResultMap (Map SomeTypeRep Dynamic)

instance Semigroup ResultMap where
  ResultMap a <> ResultMap b = ResultMap (Map.union a b)

instance Monoid ResultMap where
  mempty = ResultMap Map.empty

-- | Create mock data for a specific key type.
--
-- @mockData \@UserId [(UserId 1, testUser), (UserId 2, otherUser)]@
mockData :: forall k. (FetchKey k, Typeable (Result k))
         => [(k, Result k)] -> ResultMap
mockData pairs = ResultMap $
  Map.singleton
    (someTypeRep (Proxy @k))
    (toDyn (HM.fromList pairs :: HashMap k (Result k)))

-- | Empty mock data.
emptyMockData :: ResultMap
emptyMockData = mempty

-- | Look up a key in the mock data.
lookupMock :: forall k. (FetchKey k, Typeable (Result k))
           => ResultMap -> k -> Either SomeException (Result k)
lookupMock (ResultMap m) k =
  case Map.lookup (someTypeRep (Proxy @k)) m >>= fromDynamic of
    Just (hm :: HashMap k (Result k)) ->
      case HM.lookup k hm of
        Just v  -> Right v
        Nothing -> Left $ toException $ FetchError $
          "Mock data missing key: " <> show k
    Nothing -> Left $ toException $ FetchError $
      "No mock data for source: " <> show (someTypeRep (Proxy @k))

-- | A test-oriented MonadFetch that reads from a pre-built result map.
-- No IO, no batching, no caching. Just direct lookups.
--
-- The @m@ phantom type parameter means the same @MonadFetch m n@
-- constraints work in both production and test code. Users specify
-- @m@ at the call site (e.g. @runMockFetchT \@AppM mocks action@).
-- | @m@ is a phantom type representing the source monad (for instance selection).
newtype MockFetchT (m :: Type -> Type) n a = MockFetchT { unMockFetchT :: ResultMap -> n a }

instance Functor n => Functor (MockFetchT m n) where
  fmap f (MockFetchT g) = MockFetchT (fmap f . g)

instance Applicative n => Applicative (MockFetchT m n) where
  pure a = MockFetchT $ \_ -> pure a
  MockFetchT ff <*> MockFetchT fx = MockFetchT $ \rm ->
    ff rm <*> fx rm

instance Monad n => Monad (MockFetchT m n) where
  MockFetchT ma >>= f = MockFetchT $ \rm -> do
    a <- ma rm
    unMockFetchT (f a) rm

-- | Note: the 'DataSource m k' constraint is still required for
-- type inference, but the batchFetch is never used.
-- Only the ResultMap is consulted.
instance Monad n => MonadFetch m (MockFetchT m n) where
  fetch k = MockFetchT $ \rm ->
    case lookupMock rm k of
      Right v -> pure v
      Left ex -> throw ex

  tryFetch k = MockFetchT $ \rm ->
    pure (lookupMock rm k)

  primeCache _ _ = pure ()

instance MonadFail n => MonadFail (MockFetchT m n) where
  fail msg = MockFetchT $ \_ -> fail msg

instance MonadThrow n => MonadThrow (MockFetchT m n) where
  throwM e = MockFetchT $ \_ -> throwM e

instance MonadCatch n => MonadCatch (MockFetchT m n) where
  catch (MockFetchT f) handler = MockFetchT $ \rm ->
    catch (f rm) (\e -> unMockFetchT (handler e) rm)

-- | Run a computation against mock data.
--
-- @
-- testGetUserFeed :: IO ()
-- testGetUserFeed = do
--   let mocks = mockData \@UserId [(UserId 1, testUser)]
--            <> mockData \@PostsByAuthor [(PostsByAuthor 1, [testPost])]
--   feed <- runMockFetchT \@AppM mocks (getUserFeed (UserId 1))
--   assertEqual (feedUser feed) testUser
-- @
runMockFetchT :: ResultMap -> MockFetchT m n a -> n a
runMockFetchT rm (MockFetchT f) = f rm

-- ──────────────────────────────────────────────
-- Mock mutation support
-- ──────────────────────────────────────────────

-- | A map of mutation handlers, keyed by mutation key type.
-- Each handler is a @Dynamic@-wrapped @k -> MutationResult k@.
newtype MutationHandlers = MutationHandlers (Map SomeTypeRep Dynamic)

instance Semigroup MutationHandlers where
  MutationHandlers a <> MutationHandlers b = MutationHandlers (Map.union a b)

instance Monoid MutationHandlers where
  mempty = MutationHandlers Map.empty

-- | Empty mutation handlers.
emptyMutationHandlers :: MutationHandlers
emptyMutationHandlers = mempty

-- | Register a mock handler for a mutation key type.
--
-- @mockMutation \@UpdateUser (\\(UpdateUser uid name) -> User uid name)@
mockMutation :: forall k. (MutationKey k, Typeable (MutationResult k))
             => (k -> MutationResult k) -> MutationHandlers
mockMutation handler = MutationHandlers $
  Map.singleton
    (someTypeRep (Proxy @k))
    (toDyn handler)

-- | Look up a mutation handler.
lookupMutationHandler :: forall k. (MutationKey k, Typeable (MutationResult k))
                      => MutationHandlers -> k -> Either SomeException (MutationResult k)
lookupMutationHandler (MutationHandlers m) k =
  case Map.lookup (someTypeRep (Proxy @k)) m >>= fromDynamic of
    Just (handler :: k -> MutationResult k) -> Right (handler k)
    Nothing -> Left $ toException $ FetchError $
      "No mock mutation handler for: " <> show (someTypeRep (Proxy @k))

-- | A recorded mutation: captures the type and the existentially-wrapped
-- key for assertions. Pattern match on 'recordedMutationKey' when you
-- need to inspect the concrete key value.
data RecordedMutation = forall k. MutationKey k
  => RecordedMutation
       { recordedMutationType :: !SomeTypeRep
       , recordedMutationKey  :: !k
       }

instance Show RecordedMutation where
  show (RecordedMutation tr _) =
    "RecordedMutation " <> show tr

-- | A test-oriented monad that supports both fetches (from 'ResultMap')
-- and mutations (from 'MutationHandlers'), recording all mutations
-- for assertions.
-- | @m@ is a phantom type representing the source monad (for instance selection).
newtype MockMutateT (m :: Type -> Type) n a = MockMutateT
  { unMockMutateT :: ResultMap -> MutationHandlers -> IORef [RecordedMutation] -> n a }

instance Functor n => Functor (MockMutateT m n) where
  fmap f (MockMutateT g) = MockMutateT $ \rm mh ref -> fmap f (g rm mh ref)

instance Applicative n => Applicative (MockMutateT m n) where
  pure a = MockMutateT $ \_ _ _ -> pure a
  MockMutateT ff <*> MockMutateT fx = MockMutateT $ \rm mh ref ->
    ff rm mh ref <*> fx rm mh ref

instance Monad n => Monad (MockMutateT m n) where
  MockMutateT ma >>= f = MockMutateT $ \rm mh ref -> do
    a <- ma rm mh ref
    unMockMutateT (f a) rm mh ref

instance (Monad n, n ~ IO) => MonadFetch m (MockMutateT m n) where
  fetch k = MockMutateT $ \rm _ _ ->
    case lookupMock rm k of
      Right v -> pure v
      Left ex -> throwIO ex
  tryFetch k = MockMutateT $ \rm _ _ ->
    pure (lookupMock rm k)
  primeCache _ _ = pure ()

instance (Monad n, n ~ IO) => MonadMutate m (MockMutateT m n) where
  mutate k = MockMutateT $ \_ mh ref ->
    case lookupMutationHandler mh k of
      Right v -> do
        let tr = SomeTypeRep (typeOf k)
        atomicModifyIORef' ref $ \recs ->
          (recs <> [RecordedMutation tr k], ())
        pure v
      Left ex -> throwIO ex

  tryMutate k = MockMutateT $ \_ mh ref -> do
    result <- try $ evaluate $ lookupMutationHandler mh k
    case result of
      Right (Right v) -> do
        let tr = SomeTypeRep (typeOf k)
        atomicModifyIORef' ref $ \recs ->
          (recs <> [RecordedMutation tr k], ())
        pure (Right v)
      Right (Left ex) -> pure (Left ex)
      Left ex -> pure (Left ex)

instance MonadFail n => MonadFail (MockMutateT m n) where
  fail msg = MockMutateT $ \_ _ _ -> fail msg

instance MonadThrow n => MonadThrow (MockMutateT m n) where
  throwM e = MockMutateT $ \_ _ _ -> throwM e

instance MonadCatch n => MonadCatch (MockMutateT m n) where
  catch (MockMutateT f) handler = MockMutateT $ \rm mh ref ->
    catch (f rm mh ref) (\e -> unMockMutateT (handler e) rm mh ref)

-- | Run a computation against mock data and mutation handlers.
-- Returns the result and a list of recorded mutations.
--
-- @
-- let mocks = mockData \@UserId [(UserId 1, User 1 "alice")]
--     handlers = mockMutation \@UpdateUser (\\(UpdateUser uid name) -> User uid name)
-- (result, mutations) <- runMockMutateT \@AppM mocks handlers myAction
-- length mutations \`shouldBe\` 1
-- @
runMockMutateT :: ResultMap -> MutationHandlers -> MockMutateT m IO a -> IO (a, [RecordedMutation])
runMockMutateT rm mh (MockMutateT f) = do
  ref <- newIORef []
  a <- f rm mh ref
  mutations <- readIORef ref
  pure (a, mutations)
