{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

-- | Helpers for writing typeclass instances with less boilerplate.
--
-- = DerivingVia for transformer newtypes
--
-- If you define a newtype over 'FetchT', 'MockFetchT', 'MutateT',
-- or any of the library's monads, @GeneralizedNewtypeDeriving@ can
-- derive all the standard instances automatically:
--
-- @
-- {-\# LANGUAGE GeneralizedNewtypeDeriving, DerivingStrategies \#-}
--
-- newtype AppFetch a = AppFetch ('Fetch.Batched.FetchT' AppM a)
--   deriving newtype
--     ( Functor, Applicative, Monad
--     , MonadFail, MonadThrow, MonadCatch
--     , 'Fetch.Class.MonadFetch' AppM
--     )
-- @
--
-- For a newtype over 'MutateT' that also supports mutations:
--
-- @
-- newtype AppMutateT a = AppMutateT ('Fetch.Mutate.MutateT' AppM AppM a)
--   deriving newtype
--     ( Functor, Applicative, Monad
--     , MonadFail, MonadThrow, MonadCatch
--     , 'Fetch.Class.MonadFetch' AppM
--     , 'Fetch.Mutate.MonadMutate' AppM
--     )
-- @
--
-- For a newtype over 'MockFetchT' in tests:
--
-- @
-- newtype TestFetch a = TestFetch ('Fetch.Mock.MockFetchT' AppM IO a)
--   deriving newtype
--     ( Functor, Applicative, Monad
--     , 'Fetch.Class.MonadFetch' AppM
--     )
-- @
--
-- The library's own 'Fetch.Traced.TracedFetchT' uses this exact pattern.
--
-- == Why not FetchKey or DataSource?
--
-- 'FetchKey', 'MutationKey', and 'MemoKey' use associated type families,
-- which @DerivingVia@ cannot handle, so you must write the @type Result@
-- line manually. That said, the instances are minimal (2 lines each).
--
-- 'DataSource' methods mention @HashMap k (Result k)@ in return
-- positions, and @HashMap@'s key role is nominal, preventing coercion.
-- Use 'fetchOne' or the helpers below to reduce the boilerplate instead.
--
-- = Simpler DataSource instances
--
-- For data sources with no native batch API, implement 'fetchOne'
-- instead of 'batchFetch'. The default 'batchFetch' calls 'fetchOne'
-- for each key and assembles the results:
--
-- @
-- instance DataSource AppM UserId where
--   fetchOne (UserId uid) = lookupUserById uid
-- @
--
-- For more control over how missing keys are handled, use
-- 'optionalBatchFetch' or 'traverseBatchFetch' in your 'batchFetch'
-- implementation.
module Fetch.Deriving
  ( -- * DataSource helpers
    optionalBatchFetch
  , traverseBatchFetch
  ) where

import Fetch.Class

import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import qualified Data.List.NonEmpty as NE
import Data.Maybe (mapMaybe)

-- | Build a 'batchFetch' from a per-key lookup that may not find a result.
--
-- Missing keys are silently omitted from the 'HashMap'. The engine
-- fills them with an error via @fillUnfilled@, so callers using
-- 'fetch' see an exception and callers using 'tryFetch' see @Left@.
--
-- @
-- instance DataSource AppM UserId where
--   batchFetch = optionalBatchFetch $ \\(UserId uid) ->
--     lookupUserMaybe uid
-- @
optionalBatchFetch :: (FetchKey k, Monad m)
                   => (k -> m (Maybe (Result k)))
                   -> NonEmpty k -> m (HashMap k (Result k))
optionalBatchFetch f keys = do
  results <- traverse (\k -> fmap (\mv -> (k, mv)) (f k)) keys
  pure $ HM.fromList (mapMaybe (\(k, mv) -> fmap (\v -> (k, v)) mv) (NE.toList results))

-- | Build a 'batchFetch' from a per-key action that always succeeds.
--
-- Equivalent to the default 'batchFetch' when only 'fetchOne' is
-- implemented, but as a standalone combinator for use in custom
-- 'batchFetch' implementations (e.g. to add logging around each
-- individual fetch).
--
-- @
-- instance DataSource AppM UserId where
--   batchFetch keys = do
--     logBatchStart (length keys)
--     traverseBatchFetch lookupUser keys
-- @
traverseBatchFetch :: (FetchKey k, Monad m)
                   => (k -> m (Result k))
                   -> NonEmpty k -> m (HashMap k (Result k))
traverseBatchFetch f keys =
  HM.fromList . NE.toList <$> traverse (\k -> fmap (\v -> (k, v)) (f k)) keys
