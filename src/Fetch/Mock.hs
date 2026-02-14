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
    MockFetch
  , runMockFetch
  , ResultMap
  , mockData
  , emptyMockData
    -- * Mock mutations
  , MockMutate
  , runMockMutate
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
-- @m@ at the call site (e.g. @runMockFetch \@AppM mocks action@).
-- | @m@ is a phantom type representing the source monad (for instance selection).
newtype MockFetch (m :: Type -> Type) n a = MockFetch { unMockFetch :: ResultMap -> n a }

instance Functor n => Functor (MockFetch m n) where
  fmap f (MockFetch g) = MockFetch (fmap f . g)

instance Applicative n => Applicative (MockFetch m n) where
  pure a = MockFetch $ \_ -> pure a
  MockFetch ff <*> MockFetch fx = MockFetch $ \rm ->
    ff rm <*> fx rm

instance Monad n => Monad (MockFetch m n) where
  MockFetch ma >>= f = MockFetch $ \rm -> do
    a <- ma rm
    unMockFetch (f a) rm

-- | Note: the 'DataSource m k' constraint is still required for
-- type inference, but the batchFetch is never used.
-- Only the ResultMap is consulted.
instance Monad n => MonadFetch m (MockFetch m n) where
  fetch k = MockFetch $ \rm ->
    case lookupMock rm k of
      Right v -> pure v
      Left ex -> throw ex

  tryFetch k = MockFetch $ \rm ->
    pure (lookupMock rm k)

  primeCache _ _ = pure ()

instance MonadFail n => MonadFail (MockFetch m n) where
  fail msg = MockFetch $ \_ -> fail msg

instance MonadThrow n => MonadThrow (MockFetch m n) where
  throwM e = MockFetch $ \_ -> throwM e

instance MonadCatch n => MonadCatch (MockFetch m n) where
  catch (MockFetch f) handler = MockFetch $ \rm ->
    catch (f rm) (\e -> unMockFetch (handler e) rm)

-- | Run a computation against mock data.
--
-- @
-- testGetUserFeed :: IO ()
-- testGetUserFeed = do
--   let mocks = mockData \@UserId [(UserId 1, testUser)]
--            <> mockData \@PostsByAuthor [(PostsByAuthor 1, [testPost])]
--   feed <- runMockFetch \@AppM mocks (getUserFeed (UserId 1))
--   assertEqual (feedUser feed) testUser
-- @
runMockFetch :: ResultMap -> MockFetch m n a -> n a
runMockFetch rm (MockFetch f) = f rm

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
newtype MockMutate (m :: Type -> Type) n a = MockMutate
  { unMockMutate :: ResultMap -> MutationHandlers -> IORef [RecordedMutation] -> n a }

instance Functor n => Functor (MockMutate m n) where
  fmap f (MockMutate g) = MockMutate $ \rm mh ref -> fmap f (g rm mh ref)

instance Applicative n => Applicative (MockMutate m n) where
  pure a = MockMutate $ \_ _ _ -> pure a
  MockMutate ff <*> MockMutate fx = MockMutate $ \rm mh ref ->
    ff rm mh ref <*> fx rm mh ref

instance Monad n => Monad (MockMutate m n) where
  MockMutate ma >>= f = MockMutate $ \rm mh ref -> do
    a <- ma rm mh ref
    unMockMutate (f a) rm mh ref

instance (Monad n, n ~ IO) => MonadFetch m (MockMutate m n) where
  fetch k = MockMutate $ \rm _ _ ->
    case lookupMock rm k of
      Right v -> pure v
      Left ex -> throwIO ex
  tryFetch k = MockMutate $ \rm _ _ ->
    pure (lookupMock rm k)
  primeCache _ _ = pure ()

instance (Monad n, n ~ IO) => MonadMutate m (MockMutate m n) where
  mutate k = MockMutate $ \_ mh ref ->
    case lookupMutationHandler mh k of
      Right v -> do
        let tr = SomeTypeRep (typeOf k)
        atomicModifyIORef' ref $ \recs ->
          (recs <> [RecordedMutation tr k], ())
        pure v
      Left ex -> throwIO ex

  tryMutate k = MockMutate $ \_ mh ref -> do
    result <- try $ evaluate $ lookupMutationHandler mh k
    case result of
      Right (Right v) -> do
        let tr = SomeTypeRep (typeOf k)
        atomicModifyIORef' ref $ \recs ->
          (recs <> [RecordedMutation tr k], ())
        pure (Right v)
      Right (Left ex) -> pure (Left ex)
      Left ex -> pure (Left ex)

instance MonadFail n => MonadFail (MockMutate m n) where
  fail msg = MockMutate $ \_ _ _ -> fail msg

instance MonadThrow n => MonadThrow (MockMutate m n) where
  throwM e = MockMutate $ \_ _ _ -> throwM e

instance MonadCatch n => MonadCatch (MockMutate m n) where
  catch (MockMutate f) handler = MockMutate $ \rm mh ref ->
    catch (f rm mh ref) (\e -> unMockMutate (handler e) rm mh ref)

-- | Run a computation against mock data and mutation handlers.
-- Returns the result and a list of recorded mutations.
--
-- @
-- let mocks = mockData \@UserId [(UserId 1, User 1 "alice")]
--     handlers = mockMutation \@UpdateUser (\\(UpdateUser uid name) -> User uid name)
-- (result, mutations) <- runMockMutate \@AppM mocks handlers myAction
-- length mutations \`shouldBe\` 1
-- @
runMockMutate :: ResultMap -> MutationHandlers -> MockMutate m IO a -> IO (a, [RecordedMutation])
runMockMutate rm mh (MockMutate f) = do
  ref <- newIORef []
  a <- f rm mh ref
  mutations <- readIORef ref
  pure (a, mutations)
