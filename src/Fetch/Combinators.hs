{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

module Fetch.Combinators
  ( -- * Fetch combinators
    fetchAll
  , fetchWith
  , fetchThrough
  , fetchMap
  , fetchMaybe
  , fetchMapWith
    -- * Lifted operators
    --
    -- | Operators for working with applicative values (e.g. fetched
    -- results) without explicit binding. Prefix @.@ distinguishes
    -- them from their pure counterparts.
    --
    -- @
    -- do userAge <- fetch (UserAge uid)
    --    pure (userAge >= 18)
    -- @
    --
    -- becomes:
    --
    -- @
    -- fetch (UserAge uid) .>= pure 18
    -- @
  , (.>)
  , (.<)
  , (.>=)
  , (.<=)
  , (.==)
  , (./=)
  , (.&&)
  , (.||)
  , (.++)
    -- * Applicative pairing
  , pair
    -- * Parallel short-circuiting
  , biselect
  , pAnd
  , pOr
    -- * Sequencing
  , andThen
    -- * Applicative filter
  , filterA
    -- * Error recovery
  , withDefault
  ) where

import Fetch.Class
import Fetch.Batched (Fetch(..))

import Control.Monad.Catch (MonadCatch, catch)
import Data.Foldable (toList)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM

-- ──────────────────────────────────────────────
-- Fetch combinators
-- ──────────────────────────────────────────────

-- | Fetch all keys, preserving the container shape.
--
-- @fetchAll [k1, k2, k3]@ batches all three keys into one round.
fetchAll :: (MonadFetch m n, DataSource m k, Typeable (Result k), Traversable t)
         => t k -> n (t (Result k))
fetchAll = traverse fetch

-- | Fetch all keys and pair each with its result.
fetchWith :: (MonadFetch m n, DataSource m k, Typeable (Result k), Traversable t)
          => t k -> n (t (k, Result k))
fetchWith = traverse (\k -> (,) k <$> fetch k)

-- | Extract a key from each element, fetch, and pair back.
--
-- @fetchThrough commentAuthor comments@ fetches all authors in one
-- round and pairs each comment with its author.
fetchThrough :: (MonadFetch m n, DataSource m k, Typeable (Result k), Traversable t)
             => (a -> k) -> t a -> n (t (a, Result k))
fetchThrough toKey = traverse (\a -> (,) a <$> fetch (toKey a))

-- | Extract a key from each element, fetch, and transform.
--
-- @fetchMap commentAuthor (\\c u -> CommentView c u) comments@
fetchMap :: (MonadFetch m n, DataSource m k, Typeable (Result k), Traversable t)
         => (a -> k) -> (a -> Result k -> b) -> t a -> n (t b)
fetchMap toKey combine = traverse (\a -> combine a <$> fetch (toKey a))

-- | Fetch a key if present.
fetchMaybe :: (MonadFetch m n, DataSource m k, Typeable (Result k))
           => Maybe k -> n (Maybe (Result k))
fetchMaybe = traverse fetch

-- | Fetch a collection of keys and return a map of results.
-- Duplicates are deduplicated.
fetchMapWith :: (MonadFetch m n, DataSource m k, Typeable (Result k), Foldable f)
             => f k -> n (HashMap k (Result k))
fetchMapWith ks =
  let keys = toList ks
  in HM.fromList <$> traverse (\k -> (,) k <$> fetch k) keys

-- ──────────────────────────────────────────────
-- Lifted operators
-- ──────────────────────────────────────────────

infixr 3 .&&
infixr 2 .||
infix  4 .>, .<, .>=, .<=, .==, ./=

-- | Lifted @(>)@.
(.>) :: (Ord a, Applicative f) => f a -> f a -> f Bool
(.>) = liftA2 (>)

-- | Lifted @(<)@.
(.<) :: (Ord a, Applicative f) => f a -> f a -> f Bool
(.<) = liftA2 (<)

-- | Lifted @(>=)@.
(.>=) :: (Ord a, Applicative f) => f a -> f a -> f Bool
(.>=) = liftA2 (>=)

-- | Lifted @(<=)@.
(.<=) :: (Ord a, Applicative f) => f a -> f a -> f Bool
(.<=) = liftA2 (<=)

-- | Lifted @(==)@.
(.==) :: (Eq a, Applicative f) => f a -> f a -> f Bool
(.==) = liftA2 (==)

-- | Lifted @(/=)@.
(./=) :: (Eq a, Applicative f) => f a -> f a -> f Bool
(./=) = liftA2 (/=)

-- | Short-circuiting lifted @(&&)@. Evaluates the second argument
-- only if the first returns 'True'.
(.&&) :: Monad m => m Bool -> m Bool -> m Bool
ma .&& mb = do a <- ma; if a then mb else pure False

-- | Short-circuiting lifted @(||)@. Evaluates the second argument
-- only if the first returns 'False'.
(.||) :: Monad m => m Bool -> m Bool -> m Bool
ma .|| mb = do a <- ma; if a then pure True else mb

-- | Lifted @(++)@.
(.++) :: Applicative f => f [a] -> f [a] -> f [a]
(.++) = liftA2 (++)

-- ──────────────────────────────────────────────
-- Applicative pairing
-- ──────────────────────────────────────────────

-- | Pair two applicative computations. When used with 'Fetch',
-- both sides are batched into the same round.
--
-- @pair (fetch userKey) (fetch postKey)@
pair :: Applicative f => f a -> f b -> f (a, b)
pair = liftA2 (,)

-- ──────────────────────────────────────────────
-- Parallel short-circuiting
-- ──────────────────────────────────────────────

infixr 5 `pAnd`
infixr 4 `pOr`

-- | Select from two computations that each return @Either a@.
--
-- Both sides are probed in the same round (their fetches are batched
-- together). If either side resolves to @Left a@, the other side is
-- abandoned and the result is @Left a@. Only when both sides resolve
-- to @Right@ are the two values paired.
--
-- After each round, resolved sides are checked for early exit so
-- that multi-round computations can short-circuit as soon as
-- possible.
--
-- This is the fundamental building block for 'pAnd' and 'pOr'.
biselect :: Monad m
         => Fetch m (Either a b)
         -> Fetch m (Either a c)
         -> Fetch m (Either a (b, c))
biselect = go
  where
    go l r = Fetch $ \e -> do
      sl <- unFetch l e
      sr <- unFetch r e
      pure $ case (sl, sr) of
        -- Either side short-circuits
        (Done (Left a), _)                -> Done (Left a)
        (_, Done (Left a))                -> Done (Left a)
        -- Both sides done
        (Done (Right b), Done (Right c))  -> Done (Right (b, c))
        -- One side done, the other blocked. Keep probing the
        -- blocked side each round in case it short-circuits.
        (Done (Right b), Blocked bs kr)   -> Blocked bs (goRight b kr)
        (Blocked bs kl, Done (Right c))   -> Blocked bs (goLeft kl c)
        -- Both blocked: merge batches, recurse next round
        (Blocked bs1 kl, Blocked bs2 kr)  -> Blocked (bs1 <> bs2) (go kl kr)

    -- Left resolved to @Right b@; keep probing right for early exit.
    goRight b r = Fetch $ \e -> do
      sr <- unFetch r e
      pure $ case sr of
        Done (Left a)    -> Done (Left a)
        Done (Right c)   -> Done (Right (b, c))
        Blocked bs kr    -> Blocked bs (goRight b kr)

    -- Right resolved to @Right c@; keep probing left for early exit.
    goLeft l c = Fetch $ \e -> do
      sl <- unFetch l e
      pure $ case sl of
        Done (Left a)    -> Done (Left a)
        Done (Right b)   -> Done (Right (b, c))
        Blocked bs kl    -> Blocked bs (goLeft kl c)

-- | Parallel @(&&)@. Both sides are probed in the same round
-- (batched together). If either side returns 'False' before the
-- other completes, the result is 'False' immediately; the other
-- side's remaining rounds are not evaluated.
--
-- Compare with '.&&' which is sequential short-circuiting
-- (left-to-right), and @liftA2 (&&)@ which is concurrent but
-- never short-circuits.
--
-- @
-- pAnd (fetch (IsActive uid)) (fetch (HasPermission uid "admin"))
-- @
pAnd :: Monad m => Fetch m Bool -> Fetch m Bool -> Fetch m Bool
pAnd x y = fromEither <$> biselect (discrim <$> x) (discrim <$> y)
  where
    discrim False = Left False  -- short-circuit
    discrim True  = Right ()    -- continue
    fromEither (Left b)  = b
    fromEither (Right _) = True

-- | Parallel @(||)@. Both sides are probed in the same round
-- (batched together). If either side returns 'True' before the
-- other completes, the result is 'True' immediately; the other
-- side's remaining rounds are not evaluated.
--
-- Compare with '.||' which is sequential short-circuiting
-- (left-to-right), and @liftA2 (||)@ which is concurrent but
-- never short-circuits.
--
-- @
-- pOr (fetch (IsAdmin uid)) (fetch (IsModerator uid))
-- @
pOr :: Monad m => Fetch m Bool -> Fetch m Bool -> Fetch m Bool
pOr x y = fromEither <$> biselect (discrim <$> x) (discrim <$> y)
  where
    discrim True  = Left True   -- short-circuit
    discrim False = Right ()    -- continue
    fromEither (Left b)  = b
    fromEither (Right _) = False

-- ──────────────────────────────────────────────
-- Sequencing
-- ──────────────────────────────────────────────

-- | Monadic sequencing: run the first computation, discard its
-- result, then run the second.
--
-- In a fetch monad where @('>>')@ equals @('*>')@ (i.e. both sides
-- are batched into one round), 'andThen' forces sequential execution
-- across round boundaries.
--
-- @
-- -- These batch together (one round):
-- fetch keyA *> fetch keyB
--
-- -- This forces two rounds:
-- fetch keyA \`andThen\` fetch keyB
-- @
andThen :: Monad m => m a -> m b -> m b
andThen a b = a >>= \_ -> b

-- ──────────────────────────────────────────────
-- Applicative filter
-- ──────────────────────────────────────────────

-- | Applicative version of 'Control.Monad.filterM'.
--
-- The predicate is applied to all elements via 'traverse', so when
-- used with a fetch monad, all predicate evaluations are batched
-- into the same round.
--
-- @
-- filterA (\\uid -> fetch (IsActive uid)) userIds
-- @
filterA :: Applicative f => (a -> f Bool) -> [a] -> f [a]
filterA predicate xs =
  filt <$> traverse predicate xs
  where
    filt bools = map fst $ filter snd $ zip xs bools

-- ──────────────────────────────────────────────
-- Error recovery
-- ──────────────────────────────────────────────

-- | Run a computation; if it throws any exception, return the
-- supplied default value instead.
--
-- @
-- userName <- withDefault "unknown" (fetch (UserName uid))
-- @
withDefault :: MonadCatch m => a -> m a -> m a
withDefault d a = a `catch` \(_ :: SomeException) -> pure d
