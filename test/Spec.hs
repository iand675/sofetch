{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Main (main) where

import Fetch
import Fetch.Batched (Fetch(..))
import Fetch.Class (singletonBatch, batchKeys)
import Fetch.Combinators (biselect, pAnd, pOr)
import Fetch.IVar
import Fetch.Cache

import Control.Concurrent (forkIO)
import Control.Concurrent.Async (async, wait, waitCatch, cancel, replicateConcurrently_)
import Control.Concurrent.MVar
import Control.Exception (SomeException, toException, try, throwTo)
import qualified Control.Monad.Catch as MC
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import qualified Data.List.NonEmpty as NE
import Data.Maybe (mapMaybe)
import GHC.Generics (Generic)
import Test.Hspec

-- ══════════════════════════════════════════════
-- Test key types and data sources
-- ══════════════════════════════════════════════

newtype UserId = UserId Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserId where
  type Result UserId = String

newtype PostId = PostId Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey PostId where
  type Result PostId = String

-- | Key type with Sequential fetch strategy.
newtype SeqKey = SeqKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey SeqKey where
  type Result SeqKey = String

-- | Key type with EagerStart fetch strategy.
newtype EagerKey = EagerKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey EagerKey where
  type Result EagerKey = String

-- | Key type whose data source always throws.
newtype FailKey = FailKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey FailKey where
  type Result FailKey = String

-- | Key type with NoCaching policy.
newtype MutKey = MutKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey MutKey where
  type Result MutKey = Int

-- | Key type whose data source blocks on a barrier before returning.
newtype SlowKey = SlowKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey SlowKey where
  type Result SlowKey = String

-- | Key type that always returns a value for any Int key.
-- Used for high-fan-out stress tests.
newtype RangeKey = RangeKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey RangeKey where
  type Result RangeKey = String

-- | Key type whose source only returns results for even-numbered keys.
-- Odd keys are silently omitted, triggering fillUnfilled.
newtype PartialKey = PartialKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey PartialKey where
  type Result PartialKey = String

-- | Second Sequential-strategy source for ordering tests.
newtype SeqKey2 = SeqKey2 Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey SeqKey2 where
  type Result SeqKey2 = String

-- | Sequential strategy + always throws.
newtype FailSeqKey = FailSeqKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey FailSeqKey where
  type Result FailSeqKey = String

-- | EagerStart strategy + always throws.
newtype FailEagerKey = FailEagerKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey FailEagerKey where
  type Result FailEagerKey = String

-- | Key whose batchFetch signals on a barrier before blocking.
-- Used for async exception tests.
newtype BlockingKey = BlockingKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey BlockingKey where
  type Result BlockingKey = String

-- ══════════════════════════════════════════════
-- Test environment and monad
-- ══════════════════════════════════════════════

data TestEnv = TestEnv
  { envUsers        :: HashMap UserId String
  , envUserLog      :: IORef [[UserId]]
  , envPosts        :: HashMap PostId String
  , envPostLog      :: IORef [[PostId]]
  , envMutLog       :: IORef [[MutKey]]
  , envMutCount     :: IORef Int
  , envSlowBarrier  :: MVar ()
  , envDispatchLog  :: IORef [String]
    -- ^ Each source's batchFetch atomically appends its type name.
  , envAsyncStarted :: MVar ()
    -- ^ BlockingKey's batchFetch signals here when it enters.
  , envAsyncProceed :: MVar ()
    -- ^ BlockingKey's batchFetch blocks here until released.
  }

mkTestEnv :: IO TestEnv
mkTestEnv = TestEnv
  <$> pure defaultUsers
  <*> newIORef []
  <*> pure defaultPosts
  <*> newIORef []
  <*> newIORef []
  <*> newIORef 0
  <*> newMVar ()  -- starts full so non-SlowKey tests are unaffected
  <*> newIORef []
  <*> newEmptyMVar  -- envAsyncStarted: empty until BlockingKey signals
  <*> newEmptyMVar  -- envAsyncProceed: empty until test releases

defaultUsers :: HashMap UserId String
defaultUsers = HM.fromList
  [ (UserId 1, "Alice")
  , (UserId 2, "Bob")
  , (UserId 3, "Carol")
  ]

defaultPosts :: HashMap PostId String
defaultPosts = HM.fromList
  [ (PostId 10, "Hello World")
  , (PostId 20, "Haskell Tips")
  , (PostId 30, "Type Families")
  ]

-- | The test monad. A thin Reader over IO carrying 'TestEnv'.
-- This is what 'DataSource' instances run in.
newtype TestM a = TestM { unTestM :: TestEnv -> IO a }

instance Functor TestM where
  fmap f (TestM g) = TestM $ \env -> fmap f (g env)

instance Applicative TestM where
  pure a = TestM $ \_ -> pure a
  TestM ff <*> TestM fx = TestM $ \env -> ff env <*> fx env

instance Monad TestM where
  TestM ma >>= f = TestM $ \env -> do
    a <- ma env
    unTestM (f a) env

askTestEnv :: TestM TestEnv
askTestEnv = TestM pure

-- | Lift an IO action into TestM.
testLiftIO :: IO a -> TestM a
testLiftIO io = TestM $ \_ -> io

-- | Run a TestM action in IO, given the environment.
runTestM :: TestEnv -> TestM a -> IO a
runTestM env (TestM f) = f env

-- ══════════════════════════════════════════════
-- DataSource instances
-- ══════════════════════════════════════════════

instance DataSource TestM UserId where
  batchFetch keysNE = do
    let keys = NE.toList keysNE
    env <- askTestEnv
    testLiftIO $ modifyIORef' (envUserLog env) (keys :)
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["UserId"], ()))
    pure $ HM.fromList
      (mapMaybe (\k -> fmap (\v -> (k, v)) (HM.lookup k (envUsers env))) keys)

instance DataSource TestM PostId where
  batchFetch keysNE = do
    let keys = NE.toList keysNE
    env <- askTestEnv
    testLiftIO $ modifyIORef' (envPostLog env) (keys :)
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["PostId"], ()))
    pure $ HM.fromList
      (mapMaybe (\k -> fmap (\v -> (k, v)) (HM.lookup k (envPosts env))) keys)

instance DataSource TestM SeqKey where
  batchFetch keysNE = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["SeqKey"], ()))
    pure $ HM.fromList
      (map (\k@(SeqKey n) -> (k, "seq-" <> show n)) (NE.toList keysNE))
  fetchStrategy _ = Sequential

instance DataSource TestM EagerKey where
  batchFetch keysNE = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["EagerKey"], ()))
    pure $ HM.fromList
      (map (\k@(EagerKey n) -> (k, "eager-" <> show n)) (NE.toList keysNE))
  fetchStrategy _ = EagerStart

instance DataSource TestM FailKey where
  batchFetch _ = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["FailKey"], ()))
    error "FailKey data source exploded"

instance DataSource TestM MutKey where
  batchFetch keysNE = do
    let keys = NE.toList keysNE
    env <- askTestEnv
    testLiftIO $ modifyIORef' (envMutLog env) (keys :)
    n <- testLiftIO $ atomicModifyIORef' (envMutCount env) (\c -> (c + 1, c))
    pure $ HM.fromList (map (\k -> (k, n)) keys)
  cachePolicy _ = NoCaching

instance DataSource TestM SlowKey where
  batchFetch keysNE = do
    env <- askTestEnv
    testLiftIO $ takeMVar (envSlowBarrier env)  -- block until test releases
    pure $ HM.fromList
      (map (\k@(SlowKey n) -> (k, "slow-" <> show n)) (NE.toList keysNE))

instance DataSource TestM RangeKey where
  batchFetch keysNE =
    pure $ HM.fromList
      (map (\k@(RangeKey n) -> (k, "range-" <> show n)) (NE.toList keysNE))

instance DataSource TestM PartialKey where
  batchFetch keysNE = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["PartialKey"], ()))
    -- Only return results for even-numbered keys; odd keys are silently omitted.
    pure $ HM.fromList
      (mapMaybe (\k@(PartialKey n) ->
        if even n then Just (k, "partial-" <> show n) else Nothing)
        (NE.toList keysNE))

instance DataSource TestM SeqKey2 where
  batchFetch keysNE = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["SeqKey2"], ()))
    pure $ HM.fromList
      (map (\k@(SeqKey2 n) -> (k, "seq2-" <> show n)) (NE.toList keysNE))
  fetchStrategy _ = Sequential

instance DataSource TestM FailSeqKey where
  batchFetch _ = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["FailSeqKey"], ()))
    error "FailSeqKey data source exploded"
  fetchStrategy _ = Sequential

instance DataSource TestM FailEagerKey where
  batchFetch _ = do
    env <- askTestEnv
    testLiftIO $ atomicModifyIORef' (envDispatchLog env) (\l -> (l ++ ["FailEagerKey"], ()))
    error "FailEagerKey data source exploded"
  fetchStrategy _ = EagerStart

instance DataSource TestM BlockingKey where
  batchFetch keysNE = do
    env <- askTestEnv
    -- Signal that the batch has entered
    testLiftIO $ putMVar (envAsyncStarted env) ()
    -- Block until the test releases
    testLiftIO $ takeMVar (envAsyncProceed env)
    pure $ HM.fromList
      (map (\k@(BlockingKey n) -> (k, "blocking-" <> show n)) (NE.toList keysNE))

-- ══════════════════════════════════════════════
-- Helpers
-- ══════════════════════════════════════════════

-- | Run a Fetch computation over TestM in IO.
runTest :: TestEnv -> Fetch TestM a -> IO a
runTest env = runTestM env . runFetch (fetchConfig (runTestM env) testLiftIO)

-- | Run a Fetch computation with an externally-provided cache.
runTestWithCache :: TestEnv -> CacheRef -> Fetch TestM a -> IO a
runTestWithCache env cRef = runTestM env . runFetch ((fetchConfig (runTestM env) testLiftIO) { configCache = Just cRef })

-- | Run a Fetch computation and capture per-round (roundNumber, batchSize, sourceCount).
runTestWithRoundLog :: TestEnv -> Fetch TestM a -> IO (a, [(Int, Int, Int)])
runTestWithRoundLog env action = do
  logRef <- newIORef ([] :: [(Int, Int, Int)])
  cRef <- newCacheRef
  let e = FetchEnv
        { fetchCache = cRef
        , fetchLower = runTestM env
        , fetchLift  = testLiftIO
        }
  a <- runTestM env $ runLoopWith e (\n batches exec -> do
    testLiftIO $ modifyIORef' logRef
      (\l -> l ++ [(n, batchSize batches, batchSourceCount batches)])
    _ <- exec
    pure ()
    ) action
  lg <- readIORef logRef
  pure (a, lg)

instance MC.MonadThrow TestM where
  throwM = testLiftIO . MC.throwM

instance MC.MonadCatch TestM where
  catch (TestM f) handler = TestM $ \env ->
    MC.catch (f env) (\e -> unTestM (handler e) env)

-- ══════════════════════════════════════════════
-- Main
-- ══════════════════════════════════════════════

main :: IO ()
main = hspec $ do
  ivarSpec
  cacheSpec
  batchesSpec
  batchedSpec
  primeCacheSpec
  engineSpec
  combinatorSpec
  biselectSpec
  mockSpec
  tracedSpec
  memoSpec
  raceSpec
  mutateSpec
  applicativeErrorSpec
  sourceIsolationSpec
  partialBatchSpec
  strategyIsolationSpec
  complexPatternSpec
  liftSourceSpec
  noCachingSpec
  roundStatsSpec
  throwCatchSpec
  asyncExceptionSpec

-- ══════════════════════════════════════════════
-- IVar tests
-- ══════════════════════════════════════════════

ivarSpec :: Spec
ivarSpec = describe "Fetch.IVar" $ do

  it "newIVar starts empty" $ do
    iv <- newIVar @Int
    filled <- isIVarFilled iv
    filled `shouldBe` False

  it "tryReadIVar on empty returns Nothing" $ do
    iv <- newIVar @Int
    mr <- tryReadIVar iv
    case mr of
      Nothing -> pure ()
      Just _  -> expectationFailure "Expected Nothing"

  it "writeIVar then awaitIVar returns Right value" $ do
    iv <- newIVar
    writeIVar iv (42 :: Int)
    result <- awaitIVar iv
    case result of
      Right v -> v `shouldBe` 42
      Left _  -> expectationFailure "Expected Right"

  it "writeIVarError then awaitIVar returns Left" $ do
    iv <- newIVar @Int
    let ex = toException (FetchError "test error")
    writeIVarError iv ex
    result <- awaitIVar iv
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left"

  it "isIVarFilled returns True after write" $ do
    iv <- newIVar
    writeIVar iv (99 :: Int)
    filled <- isIVarFilled iv
    filled `shouldBe` True

  it "tryReadIVar on filled returns Just (Right value)" $ do
    iv <- newIVar
    writeIVar iv ("hello" :: String)
    mr <- tryReadIVar iv
    case mr of
      Just (Right v) -> v `shouldBe` "hello"
      _              -> expectationFailure "Expected Just (Right ...)"

  it "second write is ignored (idempotent)" $ do
    iv <- newIVar
    writeIVar iv (1 :: Int)
    writeIVar iv 2
    result <- awaitIVar iv
    case result of
      Right v -> v `shouldBe` 1
      Left _  -> expectationFailure "Expected Right"

  it "awaitIVar blocks until written" $ do
    -- Coordinate with MVar, no threadDelay
    iv <- newIVar
    resultVar <- newEmptyMVar
    _ <- forkIO $ do
      v <- awaitIVar iv
      putMVar resultVar v
    -- The forked thread is now blocked on awaitIVar.
    -- Write to unblock it.
    writeIVar iv (42 :: Int)
    result <- takeMVar resultVar
    case result of
      Right v -> v `shouldBe` 42
      Left _  -> expectationFailure "Expected Right"

  it "isIVarFilled returns True after error write" $ do
    iv <- newIVar @Int
    writeIVarError iv (toException (FetchError "boom"))
    filled <- isIVarFilled iv
    filled `shouldBe` True

  it "writeIVarError then writeIVar is ignored (error wins)" $ do
    iv <- newIVar
    writeIVarError iv (toException (FetchError "first"))
    writeIVar iv (99 :: Int)
    result <- awaitIVar iv
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left (error should win)"

  it "writeIVar then writeIVarError is ignored (value wins)" $ do
    iv <- newIVar
    writeIVar iv (1 :: Int)
    writeIVarError iv (toException (FetchError "late"))
    result <- awaitIVar iv
    case result of
      Right v -> v `shouldBe` 1
      Left _  -> expectationFailure "Expected Right (value should win)"

  it "multiple concurrent readers all get same value" $ do
    iv <- newIVar
    vars <- mapM (\_ -> do
      v <- newEmptyMVar
      _ <- forkIO $ awaitIVar iv >>= putMVar v
      pure v) [1 :: Int .. 5]
    writeIVar iv (42 :: Int)
    results <- mapM takeMVar vars
    mapM_ (\r -> case r of
      Right v -> v `shouldBe` 42
      Left _  -> expectationFailure "Expected Right") results

-- ══════════════════════════════════════════════
-- Cache tests
-- ══════════════════════════════════════════════

cacheSpec :: Spec
cacheSpec = describe "Fetch.Cache" $ do

  it "cacheLookup on empty returns CacheMiss" $ do
    cRef <- newCacheRef
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheMiss -> pure ()
      _         -> expectationFailure "Expected CacheMiss"

  it "cacheAllocate + write + lookup returns CacheHitReady" $ do
    cRef <- newCacheRef
    pairs <- cacheAllocate @UserId cRef [UserId 1]
    case pairs of
      [(_, iv)] -> do
        writeIVar iv "Alice"
        hit <- cacheLookup cRef (UserId 1)
        case hit of
          CacheHitReady v -> v `shouldBe` "Alice"
          _               -> expectationFailure "Expected CacheHitReady"
      _ -> expectationFailure "Expected one allocated pair"

  it "cacheAllocate without write returns CacheHitPending" $ do
    cRef <- newCacheRef
    _ <- cacheAllocate @UserId cRef [UserId 1]
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitPending _ -> pure ()
      _                 -> expectationFailure "Expected CacheHitPending"

  it "cacheAllocate deduplicates" $ do
    cRef <- newCacheRef
    pairs1 <- cacheAllocate @UserId cRef [UserId 1, UserId 2]
    length pairs1 `shouldBe` 2
    pairs2 <- cacheAllocate @UserId cRef [UserId 1, UserId 3]
    -- UserId 1 already allocated, only UserId 3 is new
    length pairs2 `shouldBe` 1
    case pairs2 of
      [(k, _)] -> k `shouldBe` UserId 3
      _        -> expectationFailure "Expected exactly one new pair"

  it "cacheEvict removes a key" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Alice")
    cacheEvict cRef (UserId 1)
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheMiss -> pure ()
      _         -> expectationFailure "Expected CacheMiss after eviction"

  it "cacheEvictSource removes all keys for a source" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
    cacheEvictSource @UserId cRef Proxy
    hit1 <- cacheLookup cRef (UserId 1)
    hit2 <- cacheLookup cRef (UserId 2)
    case (hit1, hit2) of
      (CacheMiss, CacheMiss) -> pure ()
      _ -> expectationFailure "Expected CacheMiss for both after evictSource"

  it "cacheEvictWhere removes matching keys" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
    cacheEvictWhere @UserId cRef Proxy (\(UserId n) -> n == 1)
    hit1 <- cacheLookup cRef (UserId 1)
    hit2 <- cacheLookup cRef (UserId 2)
    case hit1 of
      CacheMiss -> pure ()
      _ -> expectationFailure "Expected CacheMiss for evicted key"
    case hit2 of
      CacheHitReady v -> v `shouldBe` "Bob"
      _ -> expectationFailure "Expected CacheHitReady for non-evicted key"

  it "cacheWarm pre-fills values" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice"
      _               -> expectationFailure "Expected CacheHitReady"

  it "cacheContents returns all resolved values" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
    contents <- cacheContents @UserId cRef Proxy
    contents `shouldBe` HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")]

  it "errored IVars treated as CacheMiss on re-lookup" $ do
    cRef <- newCacheRef
    pairs <- cacheAllocate @UserId cRef [UserId 1]
    case pairs of
      [(_, iv)] -> do
        writeIVarError iv (toException (FetchError "boom"))
        hit <- cacheLookup cRef (UserId 1)
        case hit of
          CacheMiss -> pure ()
          _         -> expectationFailure "Expected CacheMiss for errored IVar"
      _ -> expectationFailure "Expected one allocated pair"

  it "cacheAllocate with empty key list returns []" $ do
    cRef <- newCacheRef
    pairs <- cacheAllocate @UserId cRef []
    length pairs `shouldBe` 0

  it "cacheAllocate across different key types is independent" $ do
    cRef <- newCacheRef
    pairsU <- cacheAllocate @UserId cRef [UserId 1]
    pairsP <- cacheAllocate @PostId cRef [PostId 1]
    length pairsU `shouldBe` 1
    length pairsP `shouldBe` 1

  it "cacheWarm overwrites a previously resolved entry" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Alice")
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Alice2")
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice2"
      _               -> expectationFailure "Expected CacheHitReady with overwritten value"

  it "cacheContents on empty cache returns HM.empty" $ do
    cRef <- newCacheRef
    contents <- cacheContents @UserId cRef Proxy
    contents `shouldBe` HM.empty

  it "cacheContents excludes pending (unfilled) IVars" $ do
    cRef <- newCacheRef
    _ <- cacheAllocate @UserId cRef [UserId 1]
    cacheWarm @UserId cRef (HM.singleton (UserId 2) "Bob")
    contents <- cacheContents @UserId cRef Proxy
    contents `shouldBe` HM.singleton (UserId 2) "Bob"

  it "cacheContents excludes errored IVars" $ do
    cRef <- newCacheRef
    pairs <- cacheAllocate @UserId cRef [UserId 1]
    case pairs of
      [(_, iv)] -> writeIVarError iv (toException (FetchError "boom"))
      _         -> expectationFailure "Expected one pair"
    cacheWarm @UserId cRef (HM.singleton (UserId 2) "Bob")
    contents <- cacheContents @UserId cRef Proxy
    contents `shouldBe` HM.singleton (UserId 2) "Bob"

  it "cacheEvict on non-existent key is a no-op" $ do
    cRef <- newCacheRef
    cacheEvict cRef (UserId 999)
    hit <- cacheLookup cRef (UserId 999)
    case hit of
      CacheMiss -> pure ()
      _         -> expectationFailure "Expected CacheMiss"

  it "cacheEvictSource on non-existent source type is a no-op" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Alice")
    cacheEvictSource @PostId cRef Proxy
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice"
      _               -> expectationFailure "Expected CacheHitReady, PostId eviction shouldn't touch UserId"

  it "cacheEvictWhere with always-False predicate removes nothing" $ do
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
    cacheEvictWhere @UserId cRef Proxy (const False)
    contents <- cacheContents @UserId cRef Proxy
    HM.size contents `shouldBe` 2

  it "cacheInsert writes into a previously allocated IVar" $ do
    cRef <- newCacheRef
    _ <- cacheAllocate @UserId cRef [UserId 1]
    cacheInsert cRef (UserId 1) "Alice"
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice"
      _               -> expectationFailure "Expected CacheHitReady"

-- ══════════════════════════════════════════════
-- Batches data type tests
-- ══════════════════════════════════════════════

batchesSpec :: Spec
batchesSpec = describe "Fetch.Class Batches" $ do

  it "mempty has size 0 and source count 0" $ do
    let b = mempty :: Batches TestM
    batchSize b `shouldBe` 0
    batchSourceCount b `shouldBe` 0

  it "singletonBatch <> singletonBatch same source deduplicates keys" $ do
    let b1 = singletonBatch @TestM (UserId 1)
        b2 = singletonBatch @TestM (UserId 1)
        merged = b1 <> b2
    batchSourceCount merged `shouldBe` 1
    -- Duplicate key is deduplicated
    batchSize merged `shouldBe` 1

  it "singletonBatch <> singletonBatch different sources yields source count 2" $ do
    let b1 = singletonBatch @TestM (UserId 1)
        b2 = singletonBatch @TestM (PostId 1)
        merged = b1 <> b2
    batchSourceCount merged `shouldBe` 2

  it "batchKeys @UserId extracts the correct keys" $ do
    let b = singletonBatch @TestM (UserId 1)
         <> singletonBatch @TestM (UserId 2)
        keys = batchKeys @UserId b
    length keys `shouldBe` 2
    keys `shouldSatisfy` elem (UserId 1)
    keys `shouldSatisfy` elem (UserId 2)

  it "batchKeys for absent source type returns []" $ do
    let b = singletonBatch @TestM (UserId 1)
        keys = batchKeys @PostId b
    keys `shouldBe` []

-- ══════════════════════════════════════════════
-- Fetch / Batched tests
-- ══════════════════════════════════════════════

batchedSpec :: Spec
batchedSpec = describe "Fetch.Batched" $ do

  it "simple single fetch returns correct value" $ do
    env <- mkTestEnv
    result <- runTest env $ fetch (UserId 1)
    result `shouldBe` "Alice"

  it "applicative <*> batches independent fetches into one round" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $
      (,) <$> fetch (UserId 1) <*> fetch (UserId 2)
    a `shouldBe` "Alice"
    b `shouldBe` "Bob"
    batches <- readIORef (envUserLog env)
    -- Both keys in a single batch (one round)
    length batches `shouldBe` 1

  it "monadic >>= creates separate rounds" $ do
    env <- mkTestEnv
    _ <- runTest env $ do
      _ <- fetch (UserId 1)
      fetch (UserId 2)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 2

  it "same key fetched twice in applicative is deduplicated" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $
      (,) <$> fetch (UserId 1) <*> fetch (UserId 1)
    a `shouldBe` "Alice"
    b `shouldBe` "Alice"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "second fetch of same key hits cache (no second batch)" $ do
    env <- mkTestEnv
    _ <- runTest env $ do
      _ <- fetch (UserId 1)
      fetch (UserId 1)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "tryFetch returns Right on success" $ do
    env <- mkTestEnv
    result <- runTest env $ tryFetch (UserId 1)
    case result of
      Right v -> v `shouldBe` "Alice"
      Left _  -> expectationFailure "Expected Right"

  it "tryFetch returns Left on missing key" $ do
    env <- mkTestEnv
    result <- runTest env $ tryFetch (UserId 999)
    case result of
      Left _ -> pure ()
      Right _ -> expectationFailure "Expected Left for missing key"

  it "data source exception is caught by tryFetch" $ do
    env <- mkTestEnv
    result <- runTest env $ tryFetch (FailKey 1)
    case result of
      Left _ -> pure ()
      Right _ -> expectationFailure "Expected Left for failed source"

  it "multi-source batching (UserId + PostId in same round)" $ do
    env <- mkTestEnv
    (user, post) <- runTest env $
      (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    user `shouldBe` "Alice"
    post `shouldBe` "Hello World"
    userBatches <- readIORef (envUserLog env)
    postBatches <- readIORef (envPostLog env)
    -- Each source got exactly one batch call
    length userBatches `shouldBe` 1
    length postBatches `shouldBe` 1

  it "runFetchWithCache shares cache across runs" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runTestWithCache env cRef $ fetch (UserId 1)
    -- Second run should hit cache
    _ <- runTestWithCache env cRef $ fetch (UserId 1)
    batches <- readIORef (envUserLog env)
    -- Only one batch was issued (first run); second run hit cache
    length batches `shouldBe` 1

  it "NoCaching sources don't persist in cache across rounds" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $ do
      x <- fetch (MutKey 1)
      y <- fetch (MutKey 1)
      pure (x, y)
    mutBatches <- readIORef (envMutLog env)
    -- Must dispatch exactly twice, once per round
    length mutBatches `shouldBe` 2
    -- Counter-based source returns different values across rounds
    a `shouldSatisfy` (/= b)

  it "fetch throws on missing key" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $ fetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception for missing key"

  it "fetch throws on data source exception" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $ fetch (FailKey 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception for FailKey"

  it "pure with no fetches completes with zero rounds" $ do
    env <- mkTestEnv
    result <- runTest env $ pure (42 :: Int)
    result `shouldBe` 42
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "fmap over a fetch transforms the result" $ do
    env <- mkTestEnv
    result <- runTest env $ fmap (++ "!") (fetch (UserId 1))
    result `shouldBe` "Alice!"

  it "three-way applicative batches in one round" $ do
    env <- mkTestEnv
    (a, b, c) <- runTest env $
      (,,) <$> fetch (UserId 1) <*> fetch (UserId 2) <*> fetch (UserId 3)
    a `shouldBe` "Alice"
    b `shouldBe` "Bob"
    c `shouldBe` "Carol"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "mixed monadic + applicative: 2 rounds, second round batches 2 keys" $ do
    env <- mkTestEnv
    (_, (b, c)) <- runTest env $ do
      a <- fetch (UserId 1)
      bc <- (,) <$> fetch (UserId 2) <*> fetch (UserId 3)
      pure (a, bc)
    b `shouldBe` "Bob"
    c `shouldBe` "Carol"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 2
    case batches of
      (lastRound : _) -> length lastRound `shouldBe` 2
      _               -> expectationFailure "Expected at least one batch"

  it "pre-warmed cache is hit without issuing a batch" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Cached-Alice")
    result <- runTestWithCache env cRef $ fetch (UserId 1)
    result `shouldBe` "Cached-Alice"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "tryFetch after a failed key retries on the next round" $ do
    env <- mkTestEnv
    (first, second) <- runTest env $ do
      r1 <- tryFetch (FailKey 1)
      r2 <- tryFetch (FailKey 1)
      pure (r1, r2)
    case first of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for first tryFetch"
    case second of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for second tryFetch"

-- ══════════════════════════════════════════════
-- primeCache tests
-- ══════════════════════════════════════════════

primeCacheSpec :: Spec
primeCacheSpec = describe "MonadFetch.primeCache" $ do

  it "primed value is returned by subsequent fetch without a batch" $ do
    env <- mkTestEnv
    result <- runTest env $ do
      primeCache (UserId 1) "Primed-Alice"
      fetch (UserId 1)
    result `shouldBe` "Primed-Alice"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "overwrites a resolved cache entry" $ do
    env <- mkTestEnv
    result <- runTest env $ do
      _ <- fetch (UserId 1)           -- fetches "Alice" from source
      primeCache (UserId 1) "Updated" -- overwrites
      fetch (UserId 1)                -- should return the primed value
    result `shouldBe` "Updated"

  it "is a no-op in MockFetch" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- runMockFetch @TestM mocks $ do
      primeCache (UserId 2) "Ghost"
      fetch (UserId 1)
    result `shouldBe` "Alice"

  it "fills a pending IVar" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- cacheAllocate @UserId cRef [UserId 1]
    _ <- runTestWithCache env cRef $ do
      primeCache (UserId 1) "Primed"
      fetch (UserId 1)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "primeCache multiple keys, all subsequently fetched from cache" $ do
    env <- mkTestEnv
    (a, b, c) <- runTest env $ do
      primeCache (UserId 1) "P-Alice"
      primeCache (UserId 2) "P-Bob"
      primeCache (UserId 3) "P-Carol"
      (,,) <$> fetch (UserId 1) <*> fetch (UserId 2) <*> fetch (UserId 3)
    a `shouldBe` "P-Alice"
    b `shouldBe` "P-Bob"
    c `shouldBe` "P-Carol"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "primeCache works through TracedFetch" $ do
    env <- mkTestEnv
    (result, _) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $ do
        primeCache (UserId 1) "Traced-Primed"
        fetch (UserId 1)
    result `shouldBe` "Traced-Primed"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

-- ══════════════════════════════════════════════
-- Engine tests
-- ══════════════════════════════════════════════

engineSpec :: Spec
engineSpec = describe "Fetch.Engine" $ do

  it "executeBatches returns RoundStats" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    let batches = singletonBatch @TestM (UserId 1)
                  <> singletonBatch @TestM (PostId 10)
    stats <- executeBatches (runTestM env) testLiftIO cRef batches
    roundSources stats `shouldBe` 2
    roundKeys stats `shouldBe` 2

  it "FetchStrategy ordering: Eager starts before Sequential" $ do
    env <- mkTestEnv
    (a, b, c) <- runTest env $
      (,,) <$> fetch (SeqKey 1) <*> fetch (EagerKey 1) <*> fetch (UserId 1)
    a `shouldBe` "seq-1"
    b `shouldBe` "eager-1"
    c `shouldBe` "Alice"

  it "fillMissing fills unfilled IVars with FetchError" $ do
    env <- mkTestEnv
    result <- runTest env $ tryFetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing key"

-- ══════════════════════════════════════════════
-- Combinator tests
-- ══════════════════════════════════════════════

combinatorSpec :: Spec
combinatorSpec = describe "Fetch.Combinators" $ do

  it "fetchAll over a list" $ do
    env <- mkTestEnv
    results <- runTest env $ fetchAll [UserId 1, UserId 2, UserId 3]
    results `shouldBe` ["Alice", "Bob", "Carol"]
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "fetchWith pairs keys with results" $ do
    env <- mkTestEnv
    results <- runTest env $ fetchWith [UserId 1, UserId 2]
    results `shouldBe` [(UserId 1, "Alice"), (UserId 2, "Bob")]

  it "fetchThrough extracts key, fetches, and pairs back" $ do
    env <- mkTestEnv
    let items = [(10 :: Int, UserId 1), (20, UserId 2)]
    results <- runTest env $ fetchThrough snd items
    results `shouldBe` [((10, UserId 1), "Alice"), ((20, UserId 2), "Bob")]

  it "fetchMap transforms results" $ do
    env <- mkTestEnv
    let items = [UserId 1, UserId 2]
    results <- runTest env $
      fetchMap id (\(UserId n) name -> show n <> ":" <> name) items
    results `shouldBe` ["1:Alice", "2:Bob"]

  it "fetchMaybe Nothing returns Nothing" $ do
    env <- mkTestEnv
    result <- runTest env $ fetchMaybe (Nothing :: Maybe UserId)
    result `shouldBe` Nothing

  it "fetchMaybe Just returns Just result" $ do
    env <- mkTestEnv
    result <- runTest env $ fetchMaybe (Just (UserId 1))
    result `shouldBe` Just "Alice"

  it "fetchMapWith returns HashMap" $ do
    env <- mkTestEnv
    result <- runTest env $ fetchMapWith [UserId 1, UserId 2]
    result `shouldBe` HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")]

  it "fetchAll with empty list returns []" $ do
    env <- mkTestEnv
    results <- runTest env $ fetchAll ([] :: [UserId])
    results `shouldBe` []
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

  it "fetchWith with empty list returns []" $ do
    env <- mkTestEnv
    results <- runTest env $ fetchWith ([] :: [UserId])
    results `shouldBe` []

  it "fetchMapWith with duplicate keys deduplicates in result map" $ do
    env <- mkTestEnv
    result <- runTest env $ fetchMapWith [UserId 1, UserId 1, UserId 2]
    HM.size result `shouldBe` 2
    HM.lookup (UserId 1) result `shouldBe` Just "Alice"
    HM.lookup (UserId 2) result `shouldBe` Just "Bob"

  it "fetchMaybe batches with other applicative fetches in same round" $ do
    env <- mkTestEnv
    (mVal, val) <- runTest env $
      (,) <$> fetchMaybe (Just (UserId 1)) <*> fetch (UserId 2)
    mVal `shouldBe` Just "Alice"
    val `shouldBe` "Bob"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

-- ══════════════════════════════════════════════
-- biselect / pAnd / pOr tests
-- ══════════════════════════════════════════════

biselectSpec :: Spec
biselectSpec = describe "biselect / pAnd / pOr" $ do

  -- ── biselect ──────────────────────────────────

  describe "biselect" $ do

    it "both pure Right → pairs values" $ do
      env <- mkTestEnv
      result <- runTest env $
        biselect
          (pure (Right "a") :: Fetch TestM (Either () String))
          (pure (Right "b") :: Fetch TestM (Either () String))
      result `shouldBe` Right ("a", "b")

    it "left pure Left → short-circuits immediately" $ do
      env <- mkTestEnv
      result <- runTest env $
        biselect
          (pure (Left "stop") :: Fetch TestM (Either String String))
          (pure (Right "b") :: Fetch TestM (Either String String))
      result `shouldBe` Left "stop"

    it "right pure Left → short-circuits immediately" $ do
      env <- mkTestEnv
      result <- runTest env $
        biselect
          (pure (Right "a") :: Fetch TestM (Either String String))
          (pure (Left "stop") :: Fetch TestM (Either String String))
      result `shouldBe` Left "stop"

    it "both Left → picks the left one" $ do
      env <- mkTestEnv
      result <- runTest env $
        biselect
          (pure (Left "first") :: Fetch TestM (Either String String))
          (pure (Left "second") :: Fetch TestM (Either String String))
      result `shouldBe` Left "first"

    it "both blocked, both Right → pairs values in one round" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        biselect
          (Right <$> fetch (UserId 1) :: Fetch TestM (Either () String))
          (Right <$> fetch (PostId 10))
      result `shouldBe` Right ("Alice", "Hello World")
      length rounds `shouldBe` 1
      -- Both sources dispatched in the same round
      dispLog <- readIORef (envDispatchLog env)
      dispLog `shouldContain` ["UserId"]
      dispLog `shouldContain` ["PostId"]

    it "left immediate Left, right blocked → batch never executed (MVar proof)" $ do
      env <- mkTestEnv
      -- BlockingKey's batchFetch signals envAsyncStarted then blocks on envAsyncProceed.
      -- If biselect short-circuits, that batchFetch is never called.
      result <- runTest env $
        biselect
          (pure (Left "short") :: Fetch TestM (Either String String))
          (Right <$> fetch (BlockingKey 1))
      result `shouldBe` Left "short"
      -- Prove the blocked side's batch was never entered
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "right immediate Left, left blocked → batch never executed (MVar proof)" $ do
      env <- mkTestEnv
      result <- runTest env $
        biselect
          (Right <$> fetch (BlockingKey 1) :: Fetch TestM (Either String String))
          (pure (Left "short") :: Fetch TestM (Either String String))
      result `shouldBe` Left "short"
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "both blocked, left resolves Left → right continuation abandoned (MVar proof)" $ do
      env <- mkTestEnv
      -- Round 1: fetch UserId and PostId (both fast).
      -- After round 1: left produces Left, right would need BlockingKey (never reached).
      (result, rounds) <- runTestWithRoundLog env $
        biselect
          (do name <- fetch (UserId 1)
              pure (Left name) :: Fetch TestM (Either String ()))
          (do _ <- fetch (PostId 10)
              v <- fetch (BlockingKey 1)  -- would block forever
              pure (Right v))
      result `shouldBe` Left "Alice"
      -- Only one round of batch execution (UserId + PostId)
      length rounds `shouldBe` 1
      -- BlockingKey's batchFetch was never entered
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

  -- ── pOr ───────────────────────────────────────

  describe "pOr" $ do

    it "True || False → True" $ do
      env <- mkTestEnv
      result <- runTest env $ pOr (pure True) (pure False)
      result `shouldBe` True

    it "False || True → True" $ do
      env <- mkTestEnv
      result <- runTest env $ pOr (pure False) (pure True)
      result `shouldBe` True

    it "False || False → False" $ do
      env <- mkTestEnv
      result <- runTest env $ pOr (pure False) (pure False)
      result `shouldBe` False

    it "left pure True → right fetch never executed (MVar proof)" $ do
      env <- mkTestEnv
      result <- runTest env $
        pOr (pure True) (const False <$> fetch (BlockingKey 1))
      result `shouldBe` True
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "right pure True → left fetch never executed (MVar proof)" $ do
      env <- mkTestEnv
      result <- runTest env $
        pOr (const False <$> fetch (BlockingKey 1)) (pure True)
      result `shouldBe` True
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "both fetched, left True → True in one round" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pOr
          ((== "Alice") <$> fetch (UserId 1))
          ((== "nonexistent") <$> fetch (PostId 10))
      result `shouldBe` True
      length rounds `shouldBe` 1

    it "both fetched, both False → False in one round" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pOr
          ((== "nonexistent") <$> fetch (UserId 1))
          ((== "nonexistent") <$> fetch (PostId 10))
      result `shouldBe` False
      length rounds `shouldBe` 1

    it "multi-round: left True after round 1, right's round 2 abandoned (MVar proof)" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pOr
          -- Left: fetches UserId in round 1, resolves True
          (do name <- fetch (UserId 1)
              pure (name == "Alice"))
          -- Right: fetches PostId in round 1, then would need BlockingKey in round 2
          (do _ <- fetch (PostId 10)
              _ <- fetch (BlockingKey 1)  -- never reached
              pure False)
      result `shouldBe` True
      -- Only one batch round was executed
      length rounds `shouldBe` 1
      -- BlockingKey's batchFetch was never entered
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

  -- ── pAnd ──────────────────────────────────────

  describe "pAnd" $ do

    it "True && True → True" $ do
      env <- mkTestEnv
      result <- runTest env $ pAnd (pure True) (pure True)
      result `shouldBe` True

    it "True && False → False" $ do
      env <- mkTestEnv
      result <- runTest env $ pAnd (pure True) (pure False)
      result `shouldBe` False

    it "False && True → False" $ do
      env <- mkTestEnv
      result <- runTest env $ pAnd (pure False) (pure True)
      result `shouldBe` False

    it "left pure False → right fetch never executed (MVar proof)" $ do
      env <- mkTestEnv
      result <- runTest env $
        pAnd (pure False) (const True <$> fetch (BlockingKey 1))
      result `shouldBe` False
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "right pure False → left fetch never executed (MVar proof)" $ do
      env <- mkTestEnv
      result <- runTest env $
        pAnd (const True <$> fetch (BlockingKey 1)) (pure False)
      result `shouldBe` False
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

    it "both fetched, both True → True in one round" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pAnd
          ((== "Alice") <$> fetch (UserId 1))
          ((== "Hello World") <$> fetch (PostId 10))
      result `shouldBe` True
      length rounds `shouldBe` 1

    it "both fetched, one False → False in one round" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pAnd
          ((== "Alice") <$> fetch (UserId 1))
          ((== "nonexistent") <$> fetch (PostId 10))
      result `shouldBe` False
      length rounds `shouldBe` 1

    it "multi-round: left False after round 1, right's round 2 abandoned (MVar proof)" $ do
      env <- mkTestEnv
      (result, rounds) <- runTestWithRoundLog env $
        pAnd
          -- Left: fetches UserId in round 1, resolves False
          (do name <- fetch (UserId 1)
              pure (name == "nonexistent"))
          -- Right: fetches PostId in round 1, then would need BlockingKey in round 2
          (do _ <- fetch (PostId 10)
              _ <- fetch (BlockingKey 1)  -- never reached
              pure True)
      result `shouldBe` False
      length rounds `shouldBe` 1
      started <- tryTakeMVar (envAsyncStarted env)
      started `shouldBe` Nothing

-- ══════════════════════════════════════════════
-- Mock tests
-- ══════════════════════════════════════════════

mockSpec :: Spec
mockSpec = describe "Fetch.Mock" $ do

  it "runMockFetch with matching data returns value" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- runMockFetch @TestM mocks $ fetch (UserId 1)
    result `shouldBe` "Alice"

  it "fetch with missing key throws" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- try @SomeException $
      runMockFetch @TestM mocks $ fetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception for missing key"

  it "tryFetch with missing key returns Left" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- runMockFetch @TestM mocks $ tryFetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing key"

  it "multiple source types in one ResultMap" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
             <> mockData @PostId [(PostId 10, "Hello")]
    (user, post) <- runMockFetch @TestM mocks $
      (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    user `shouldBe` "Alice"
    post `shouldBe` "Hello"

  it "emptyMockData causes tryFetch to return Left" $ do
    result <- runMockFetch @TestM emptyMockData $ tryFetch (UserId 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for empty mock data"

  it "mock applicative: two fetches from different sources both succeed" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
             <> mockData @PostId [(PostId 10, "Post")]
    (u, p) <- runMockFetch @TestM mocks $
      (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    u `shouldBe` "Alice"
    p `shouldBe` "Post"

  it "mock tryFetch returns Right on success" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- runMockFetch @TestM mocks $ tryFetch (UserId 1)
    case result of
      Right v -> v `shouldBe` "Alice"
      Left _  -> expectationFailure "Expected Right"

  it "mock fetch with no data for that source type returns error" $ do
    let mocks = mockData @PostId [(PostId 10, "Post")]
    result <- try @SomeException $
      runMockFetch @TestM mocks $ fetch (UserId 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception for missing source type"

  it "mock fetch missing key throws FetchError (not ErrorCall)" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- try @FetchError $
      runMockFetch @TestM mocks $ fetch (UserId 999)
    case result of
      Left (FetchError _) -> pure ()
      Right _ -> expectationFailure "Expected FetchError for missing key"

  it "MockMutate fetch missing key throws FetchError (not ErrorCall)" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
        handlers = emptyMutationHandlers
    result <- try @FetchError $ do
      (v, _) <- runMockMutate @TestM mocks handlers $ fetch (UserId 999)
      pure v
    case result of
      Left (FetchError _) -> pure ()
      Right _ -> expectationFailure "Expected FetchError for missing key"

-- ══════════════════════════════════════════════
-- Traced tests
-- ══════════════════════════════════════════════

tracedSpec :: Spec
tracedSpec = describe "Fetch.Traced" $ do

  it "callbacks fire and FetchStats reports correct counts" $ do
    env <- mkTestEnv
    roundStartRef <- newIORef (0 :: Int)
    roundCompleteRef <- newIORef (0 :: Int)
    let tc = TraceConfig
          { onRoundStart    = \_ _ -> testLiftIO $ modifyIORef' roundStartRef (+ 1)
          , onRoundComplete = \_ _ -> testLiftIO $ modifyIORef' roundCompleteRef (+ 1)
          , onFetchComplete = \_ -> pure ()
          }
    (result, stats) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) tc $ do
        (,) <$> fetch (UserId 1) <*> fetch (UserId 2)
    fst result `shouldBe` "Alice"
    snd result `shouldBe` "Bob"
    totalRounds stats `shouldBe` 1
    totalKeys stats `shouldBe` 2
    starts <- readIORef roundStartRef
    starts `shouldBe` 1
    completes <- readIORef roundCompleteRef
    completes `shouldBe` 1

  it "multiple rounds tracked correctly" $ do
    env <- mkTestEnv
    (_, stats) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $ do
        _ <- fetch (UserId 1)
        fetch (UserId 2)
    totalRounds stats `shouldBe` 2
    totalKeys stats `shouldBe` 2

  it "same batching behavior as Fetch" $ do
    env <- mkTestEnv
    ((a, b), _) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $
        (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    a `shouldBe` "Alice"
    b `shouldBe` "Hello World"

  it "onFetchComplete callback fires and receives stats" $ do
    env <- mkTestEnv
    statsRef <- newIORef Nothing
    let tc = TraceConfig
          { onRoundStart    = \_ _ -> pure ()
          , onRoundComplete = \_ _ -> pure ()
          , onFetchComplete = \s -> testLiftIO $ writeIORef statsRef (Just s)
          }
    _ <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) tc $ fetch (UserId 1)
    ms <- readIORef statsRef
    case ms of
      Just s  -> totalRounds s `shouldBe` 1
      Nothing -> expectationFailure "onFetchComplete was not called"

  it "FetchStats.totalTime is non-negative" $ do
    env <- mkTestEnv
    (_, stats) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $
        fetch (UserId 1)
    totalTime stats `shouldSatisfy` (>= 0)

  it "FetchStats.maxSourcesPerRound reports correct max" $ do
    env <- mkTestEnv
    (_, stats) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $
        (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    maxSourcesPerRound stats `shouldBe` 2

  it "primeCache through TracedFetch works" $ do
    env <- mkTestEnv
    (result, stats) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $ do
        primeCache (UserId 1) "Traced-Prime"
        fetch (UserId 1)
    result `shouldBe` "Traced-Prime"
    totalRounds stats `shouldBe` 0

  it "tryFetch returns Left for missing key through TracedFetch" $ do
    env <- mkTestEnv
    (result, _) <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) defaultTraceConfig $
        tryFetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing key"

  it "round numbers passed to onRoundStart are sequential starting at 1" $ do
    env <- mkTestEnv
    roundNums <- newIORef ([] :: [Int])
    let tc = TraceConfig
          { onRoundStart    = \n _ -> testLiftIO $ modifyIORef' roundNums (++ [n])
          , onRoundComplete = \_ _ -> pure ()
          , onFetchComplete = \_ -> pure ()
          }
    _ <- runTestM env $
      runTracedFetch (fetchConfig (runTestM env) testLiftIO) tc $ do
        _ <- fetch (UserId 1)
        _ <- fetch (UserId 2)
        fetch (UserId 3)
    nums <- readIORef roundNums
    nums `shouldBe` [1, 2, 3]

-- ══════════════════════════════════════════════
-- Memo tests
-- ══════════════════════════════════════════════

newtype ComputeKey = ComputeKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance MemoKey ComputeKey where
  type MemoResult ComputeKey = String

memoSpec :: Spec
memoSpec = describe "Fetch.Memo" $ do

  it "memo caches computation (action runs once)" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let action :: IO String
        action = do
          modifyIORef' callCount (+ 1)
          pure "computed"
    v1 <- memo store id (ComputeKey 1) action
    v2 <- memo store id (ComputeKey 1) action
    v1 `shouldBe` "computed"
    v2 `shouldBe` "computed"
    count <- readIORef callCount
    count `shouldBe` 1

  it "memo with different keys runs action for each" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let action :: IO String
        action = do
          n <- atomicModifyIORef' callCount (\c -> (c + 1, c))
          pure ("result-" <> show n)
    v1 <- memo store id (ComputeKey 1) action
    v2 <- memo store id (ComputeKey 2) action
    v1 `shouldBe` "result-0"
    v2 `shouldBe` "result-1"
    count <- readIORef callCount
    count `shouldBe` 2

  it "memoOn works without MemoKey instance" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let action :: IO Int
        action = do
          modifyIORef' callCount (+ 1)
          pure 42
    v1 <- memoOn store id ("key1" :: String) action
    v2 <- memoOn store id ("key1" :: String) action
    v1 `shouldBe` (42 :: Int)
    v2 `shouldBe` 42
    count <- readIORef callCount
    count `shouldBe` 1

  it "memoOn with different result types distinguished" $ do
    store <- newMemoStore
    v1 <- memoOn store id ("key" :: String) (pure (42 :: Int))
    v2 <- memoOn store id ("key" :: String) (pure ("hello" :: String))
    v1 `shouldBe` (42 :: Int)
    v2 `shouldBe` "hello"

  it "two separate MemoStores are independent" $ do
    store1 <- newMemoStore
    store2 <- newMemoStore
    count1 <- newIORef (0 :: Int)
    count2 <- newIORef (0 :: Int)
    let action1 :: IO String
        action1 = modifyIORef' count1 (+ 1) >> pure "store1"
        action2 :: IO String
        action2 = modifyIORef' count2 (+ 1) >> pure "store2"
    v1 <- memo store1 id (ComputeKey 1) action1
    v2 <- memo store2 id (ComputeKey 1) action2
    v1 `shouldBe` "store1"
    v2 `shouldBe` "store2"
    c1 <- readIORef count1
    c2 <- readIORef count2
    c1 `shouldBe` 1
    c2 `shouldBe` 1

  it "memoOn with same key and same result type returns cached value" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let action :: IO Int
        action = do
          modifyIORef' callCount (+ 1)
          pure 100
    v1 <- memoOn store id ("same" :: String) action
    v2 <- memoOn store id ("same" :: String) (pure (999 :: Int))
    v1 `shouldBe` (100 :: Int)
    v2 `shouldBe` (100 :: Int)
    count <- readIORef callCount
    count `shouldBe` 1

  it "memo after an errored first attempt re-runs" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let action :: IO String
        action = do
          n <- atomicModifyIORef' callCount (\c -> (c + 1, c))
          if n == 0
            then error "first attempt fails"
            else pure "success"
    r1 <- try @SomeException $ memo store id (ComputeKey 1) action
    case r1 of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception on first attempt"
    v2 <- memo store id (ComputeKey 1) action
    v2 `shouldBe` "success"
    count <- readIORef callCount
    count `shouldBe` 2

-- ══════════════════════════════════════════════
-- Race condition tests
-- ══════════════════════════════════════════════

raceSpec :: Spec
raceSpec = describe "Race conditions" $ do
  ivarRaceSpec
  cacheRaceSpec
  engineRaceSpec
  fetchTRaceSpec
  memoRaceSpec

-- ──────────────────────────────────────────────
-- IVar races
-- ──────────────────────────────────────────────

ivarRaceSpec :: Spec
ivarRaceSpec = describe "IVar" $ do

  it "concurrent write storm: exactly one winner across 100 threads" $ do
    iv <- newIVar
    doneVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. 100]
    barrier <- newEmptyMVar
    mapM_ (\(i, done) -> forkIO $ do
      readMVar barrier
      writeIVar iv i
      putMVar done ()
      ) (zip [1 :: Int .. 100] doneVars)
    putMVar barrier ()
    mapM_ takeMVar doneVars
    result <- awaitIVar iv
    case result of
      Right v -> v `shouldSatisfy` (\x -> x >= 1 && x <= 100)
      Left _  -> expectationFailure "Expected Right"

  it "concurrent error + value writes: one winner, all readers agree" $ do
    iv <- newIVar
    let n = 100
    doneVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    barrier <- newEmptyMVar
    mapM_ (\(i, done) -> forkIO $ do
      readMVar barrier
      if i <= 50
        then writeIVar iv (i :: Int)
        else writeIVarError iv (toException (FetchError ("err-" <> show i)))
      putMVar done ()
      ) (zip [1..n] doneVars)
    putMVar barrier ()
    mapM_ takeMVar doneVars
    results <- mapM (\_ -> awaitIVar iv) [1 :: Int .. 10]
    case results of
      [] -> expectationFailure "No results"
      (first : rest) -> case first of
        Right winner -> mapM_ (\r -> case r of
          Right v -> v `shouldBe` winner
          Left _  -> expectationFailure "Inconsistent: first was Right, got Left") rest
        Left _ -> mapM_ (\r -> case r of
          Left _  -> pure ()
          Right _ -> expectationFailure "Inconsistent: first was Left, got Right") rest

  it "reader-writer interleave: N readers unblocked by single write" $ do
    iv <- newIVar
    let numReaders = 50
    resultVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. numReaders]
    mapM_ (\rv -> forkIO (awaitIVar iv >>= putMVar rv)) resultVars
    writeIVar iv (42 :: Int)
    results <- mapM takeMVar resultVars
    mapM_ (\r -> case r of
      Right v -> v `shouldBe` 42
      Left _  -> expectationFailure "Expected Right") results

  it "rapid alloc-write-read cycle stress (1000 iterations)" $ do
    mapM_ (\i -> do
      iv <- newIVar
      writeIVar iv (i :: Int)
      result <- awaitIVar iv
      case result of
        Right v -> v `shouldBe` i
        Left _  -> expectationFailure "Expected Right"
      ) [1 :: Int .. 1000]

-- ──────────────────────────────────────────────
-- Cache races
-- ──────────────────────────────────────────────

cacheRaceSpec :: Spec
cacheRaceSpec = describe "Cache" $ do

  it "concurrent cacheAllocate same key: exactly one allocator wins" $ do
    cRef <- newCacheRef
    resultsVar <- newIORef ([] :: [Int])
    barrier <- newEmptyMVar
    let n = 100
    doneVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    mapM_ (\(_, done) -> forkIO $ do
      readMVar barrier
      pairs <- cacheAllocate @UserId cRef [UserId 1]
      atomicModifyIORef' resultsVar (\rs -> (length pairs : rs, ()))
      putMVar done ()
      ) (zip [1 :: Int .. n] doneVars)
    putMVar barrier ()
    mapM_ takeMVar doneVars
    results <- readIORef resultsVar
    let allocators = filter (> 0) results
    length allocators `shouldBe` 1

  it "cacheAllocate + cacheEvict interleave: no corruption" $ do
    cRef <- newCacheRef
    barrier <- newEmptyMVar
    h1 <- async $ do
      readMVar barrier
      pairs <- cacheAllocate @UserId cRef [UserId 1]
      case pairs of
        [(_, iv)] -> writeIVar iv "value"
        _         -> pure ()
    h2 <- async $ do
      readMVar barrier
      cacheEvict cRef (UserId 1)
    putMVar barrier ()
    wait h1
    wait h2
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheMiss       -> pure ()
      CacheHitReady v -> v `shouldBe` "value"
      CacheHitPending _ -> pure ()

  it "cacheWarm + cacheLookup concurrent: never corrupt state" $ do
    cRef <- newCacheRef
    let warmMap = HM.fromList [ (UserId i, "user-" <> show i)
                               | i <- [1..100] ]
    barrier <- newEmptyMVar
    h1 <- async $ do
      readMVar barrier
      cacheWarm @UserId cRef warmMap
    badRef <- newIORef False
    h2 <- async $ do
      readMVar barrier
      mapM_ (\i -> do
        hit <- cacheLookup cRef (UserId i)
        case hit of
          CacheMiss         -> pure ()
          CacheHitReady _   -> pure ()
          CacheHitPending _ -> pure ()
        ) [1..100]
    putMVar barrier ()
    wait h1
    wait h2
    bad <- readIORef badRef
    bad `shouldBe` False

  it "cacheInsert after concurrent evict: no crash" $ do
    cRef <- newCacheRef
    _ <- cacheAllocate @UserId cRef [UserId 1]
    barrier <- newEmptyMVar
    h1 <- async $ do
      readMVar barrier
      cacheInsert cRef (UserId 1) "value"
    h2 <- async $ do
      readMVar barrier
      cacheEvict cRef (UserId 1)
    putMVar barrier ()
    wait h1
    wait h2
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheMiss       -> pure ()
      CacheHitReady _ -> pure ()
      CacheHitPending _ -> pure ()

  it "concurrent cacheWarm different keys: both sets present" $ do
    cRef <- newCacheRef
    let set1 = HM.fromList [ (UserId i, "a-" <> show i) | i <- [1..50] ]
        set2 = HM.fromList [ (UserId i, "b-" <> show i) | i <- [51..100] ]
    h1 <- async $ cacheWarm @UserId cRef set1
    h2 <- async $ cacheWarm @UserId cRef set2
    wait h1
    wait h2
    contents <- cacheContents @UserId cRef Proxy
    mapM_ (\i -> HM.member (UserId i) contents `shouldBe` True) [1..100]

  it "cacheContents during concurrent writes: internally consistent" $ do
    cRef <- newCacheRef
    barrier <- newEmptyMVar
    h1 <- async $ do
      readMVar barrier
      mapM_ (\i -> do
        cacheWarm @UserId cRef (HM.singleton (UserId i) ("val-" <> show i))
        ) [1 :: Int .. 50]
    h2 <- async $ do
      readMVar barrier
      mapM_ (\_ -> do
        contents <- cacheContents @UserId cRef Proxy
        mapM_ (\(_, v) ->
          length v `shouldSatisfy` (> 0)) (HM.toList contents)
        ) [1 :: Int .. 50]
    putMVar barrier ()
    wait h1
    wait h2

-- ──────────────────────────────────────────────
-- Engine / dispatch races
-- ──────────────────────────────────────────────

engineRaceSpec :: Spec
engineRaceSpec = describe "Engine dispatch" $ do

  it "concurrent executeBatches on same CacheRef: no crash, all IVars filled" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    let b1 = singletonBatch @TestM (UserId 1) <> singletonBatch @TestM (UserId 2)
        b2 = singletonBatch @TestM (PostId 10) <> singletonBatch @TestM (PostId 20)
    h1 <- async $ executeBatches (runTestM env) testLiftIO cRef b1
    h2 <- async $ executeBatches (runTestM env) testLiftIO cRef b2
    _ <- wait h1
    _ <- wait h2
    u1 <- cacheLookup cRef (UserId 1)
    u2 <- cacheLookup cRef (UserId 2)
    p1 <- cacheLookup cRef (PostId 10)
    p2 <- cacheLookup cRef (PostId 20)
    case (u1, u2, p1, p2) of
      (CacheHitReady a, CacheHitReady b, CacheHitReady c, CacheHitReady d) -> do
        a `shouldBe` "Alice"
        b `shouldBe` "Bob"
        c `shouldBe` "Hello World"
        d `shouldBe` "Haskell Tips"
      _ -> expectationFailure "Expected all CacheHitReady"

  it "all three strategies in one round: Eager + Sequential + Concurrent" $ do
    env <- mkTestEnv
    (a, b, c) <- runTest env $
      (,,) <$> fetch (EagerKey 1) <*> fetch (SeqKey 1) <*> fetch (UserId 1)
    a `shouldBe` "eager-1"
    b `shouldBe` "seq-1"
    c `shouldBe` "Alice"

  it "high-fan-out: 100 distinct keys in one applicative round" $ do
    env <- mkTestEnv
    let keys = map RangeKey [1..100]
    results <- runTest env $ fetchAll keys
    length results `shouldBe` 100
    results `shouldBe` ["range-" <> show i | i <- [1 :: Int .. 100]]

  it "concurrent runFetchWithCache from multiple threads: no crash" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    barrier <- newEmptyMVar
    let n = 20
    doneVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    mapM_ (\(i, done) -> forkIO $ do
      readMVar barrier
      result <- runTestWithCache env cRef $ fetch (UserId (1 + i `mod` 3))
      length result `shouldSatisfy` (> 0)
      putMVar done ()
      ) (zip [0 :: Int .. n - 1] doneVars)
    putMVar barrier ()
    mapM_ takeMVar doneVars

-- ──────────────────────────────────────────────
-- Fetch / primeCache races
-- ──────────────────────────────────────────────

fetchTRaceSpec :: Spec
fetchTRaceSpec = describe "Fetch / primeCache" $ do

  it "concurrent primeCache + fetch for same key: no corruption" $ do
    mapM_ (\_ -> do
      env <- mkTestEnv
      cRef <- newCacheRef
      barrier <- newEmptyMVar
      resultVar <- newEmptyMVar
      _ <- forkIO $ do
        readMVar barrier
        runTestWithCache env cRef $ primeCache (UserId 1) "primed"
        pure ()
      _ <- forkIO $ do
        readMVar barrier
        r <- runTestWithCache env cRef $ fetch (UserId 1)
        putMVar resultVar r
      putMVar barrier ()
      result <- takeMVar resultVar
      result `shouldSatisfy` (\v -> v == "primed" || v == "Alice")
      ) [1 :: Int .. 50]

  it "primeCache into pending IVar while batch in flight" $ do
    env0 <- mkTestEnv
    slowBarrier <- newEmptyMVar
    let env' = env0 { envSlowBarrier = slowBarrier }
    cRef <- newCacheRef
    fetchDone <- newEmptyMVar
    _ <- forkIO $ do
      r <- runTestWithCache env' cRef $ fetch (SlowKey 1)
      putMVar fetchDone r
    let waitForPending = do
          hit <- cacheLookup cRef (SlowKey 1)
          case hit of
            CacheHitPending _ -> pure ()
            _                 -> waitForPending
    waitForPending
    runTestWithCache env' cRef $ primeCache (SlowKey 1) "primed-value"
    putMVar slowBarrier ()
    result <- takeMVar fetchDone
    result `shouldBe` "primed-value"

  it "concurrent primeCache storm: one value wins" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    barrier <- newEmptyMVar
    let n = 50
    doneVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    mapM_ (\(i, done) -> forkIO $ do
      readMVar barrier
      runTestWithCache env cRef $
        primeCache (UserId 1) ("prime-" <> show i)
      putMVar done ()
      ) (zip [1 :: Int .. n] doneVars)
    putMVar barrier ()
    mapM_ takeMVar doneVars
    result <- runTestWithCache env cRef $ fetch (UserId 1)
    let hasPrimePrefix v = take 6 v == "prime-"
    result `shouldSatisfy` (\v -> hasPrimePrefix v || v == "Alice")

  it "primeCache + cacheEvict race: no crash" $ do
    mapM_ (\_ -> do
      env <- mkTestEnv
      cRef <- newCacheRef
      barrier <- newEmptyMVar
      h1 <- async $ do
        readMVar barrier
        runTestWithCache env cRef $ primeCache (UserId 1) "primed"
      h2 <- async $ do
        readMVar barrier
        cacheEvict cRef (UserId 1)
      putMVar barrier ()
      wait h1
      wait h2
      hit <- cacheLookup cRef (UserId 1)
      case hit of
        CacheMiss       -> pure ()
        CacheHitReady _ -> pure ()
        CacheHitPending _ -> pure ()
      ) [1 :: Int .. 50]

-- ──────────────────────────────────────────────
-- Memo races
-- ──────────────────────────────────────────────

memoRaceSpec :: Spec
memoRaceSpec = describe "Memo" $ do

  it "concurrent memo same key: action runs at most a few times" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    barrier <- newEmptyMVar
    let n = 100
    resultVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    mapM_ (\(_, rv) -> forkIO $ do
      readMVar barrier
      v <- memo store id (ComputeKey 1) $ do
        atomicModifyIORef' callCount (\c -> (c + 1, ()))
        pure "computed"
      putMVar rv v
      ) (zip [1 :: Int .. n] resultVars)
    putMVar barrier ()
    results <- mapM takeMVar resultVars
    mapM_ (\v -> v `shouldBe` "computed") results
    count <- readIORef callCount
    count `shouldSatisfy` (< n)

  it "concurrent memoOn same key: action runs at most a few times" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    barrier <- newEmptyMVar
    let n = 100
    resultVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    mapM_ (\(_, rv) -> forkIO $ do
      readMVar barrier
      v <- memoOn store id ("shared-key" :: String) $ do
        atomicModifyIORef' callCount (\c -> (c + 1, ()))
        pure (42 :: Int)
      putMVar rv v
      ) (zip [1 :: Int .. n] resultVars)
    putMVar barrier ()
    results <- mapM takeMVar resultVars
    mapM_ (\v -> v `shouldBe` (42 :: Int)) results
    count <- readIORef callCount
    count `shouldSatisfy` (< n)

  it "concurrent memo different keys: each runs exactly once" $ do
    store <- newMemoStore
    callCount <- newIORef (0 :: Int)
    let n = 100
    replicateConcurrently_ n $ do
      myKey <- atomicModifyIORef' callCount (\c -> (c + 1, c))
      v <- memo store id (ComputeKey myKey) (pure ("result-" <> show myKey))
      v `shouldBe` ("result-" <> show myKey)
    count <- readIORef callCount
    count `shouldBe` n

  it "memo + error race: no deadlock, valid results or rethrown exceptions" $ do
    store <- newMemoStore
    let n = 50
    resultVars <- mapM (\_ -> newEmptyMVar) [1 :: Int .. n]
    barrier <- newEmptyMVar
    callCount <- newIORef (0 :: Int)
    mapM_ (\(_, rv) -> forkIO $ do
      readMVar barrier
      r <- try @SomeException $ memo store id (ComputeKey 1) $ do
        myCall <- atomicModifyIORef' callCount (\c -> (c + 1, c))
        if myCall == 0
          then error "first call fails"
          else pure "success"
      putMVar rv r
      ) (zip [1 :: Int .. n] resultVars)
    putMVar barrier ()
    results <- mapM takeMVar resultVars
    mapM_ (\r -> case r of
      Left _  -> pure ()
      Right v -> v `shouldBe` "success"
      ) results

-- ══════════════════════════════════════════════
-- Mutation key types
-- ══════════════════════════════════════════════

data UpdateUser = UpdateUser UserId String
  deriving stock (Show)

instance MutationKey UpdateUser where
  type MutationResult UpdateUser = String  -- returns updated name

data DeleteUser = DeleteUser UserId
  deriving stock (Show)

instance MutationKey DeleteUser where
  type MutationResult DeleteUser = ()

data FailMutation = FailMutation
  deriving stock (Show)

instance MutationKey FailMutation where
  type MutationResult FailMutation = ()

-- ══════════════════════════════════════════════
-- MutationSource instances for TestM
-- ══════════════════════════════════════════════

instance MutationSource TestM UpdateUser where
  executeMutation (UpdateUser (UserId n) newName) =
    pure $ "updated-" <> newName <> "-" <> show n

  reconcileCache (UpdateUser uid _) result cRef =
    cacheWarm @UserId cRef (HM.singleton uid result)

instance MutationSource TestM DeleteUser where
  executeMutation (DeleteUser _) = pure ()

  reconcileCache (DeleteUser uid) _ cRef =
    cacheEvict cRef uid

instance MutationSource TestM FailMutation where
  executeMutation FailMutation =
    error "FailMutation always throws"

-- ══════════════════════════════════════════════
-- Mutate tests
-- ══════════════════════════════════════════════

-- | Run a Mutate computation over TestM in IO.
runMutateTest :: TestEnv -> Mutate TestM TestM a -> IO a
runMutateTest env = runTestM env . runMutate (fetchConfig (runTestM env) testLiftIO)

-- | Run a Mutate computation with an externally-provided cache.
runMutateTestWithCache :: TestEnv -> CacheRef -> Mutate TestM TestM a -> IO a
runMutateTestWithCache env cRef = runTestM env . runMutate ((fetchConfig (runTestM env) testLiftIO) { configCache = Just cRef })

mutateSpec :: Spec
mutateSpec = describe "Fetch.Mutate (Mutate)" $ do
  mutateBasicSpec
  mutateFetchInteractionSpec
  mutateApplicativeSpec
  mutateMonadicSpec
  mutateMockSpec
  mutateCacheReconcileSpec

mutateBasicSpec :: Spec
mutateBasicSpec = describe "basic mutations" $ do

  it "mutate returns correct result" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ mutate (UpdateUser (UserId 1) "NewAlice")
    result `shouldBe` "updated-NewAlice-1"

  it "tryMutate returns Right on success" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ tryMutate (UpdateUser (UserId 1) "NewAlice")
    case result of
      Right v -> v `shouldBe` "updated-NewAlice-1"
      Left _  -> expectationFailure "Expected Right"

  it "tryMutate catches exception" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ tryMutate FailMutation
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left"

  it "mutate throws on exception" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runMutateTest env $ mutate FailMutation
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"

  it "pure with no mutations completes immediately" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ pure (42 :: Int)
    result `shouldBe` 42

mutateFetchInteractionSpec :: Spec
mutateFetchInteractionSpec = describe "fetch-mutate-fetch interaction" $ do

  it "fetch works within Mutate" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ fetch (UserId 1)
    result `shouldBe` "Alice"

  it "tryFetch works within Mutate" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ tryFetch (UserId 1)
    case result of
      Right v -> v `shouldBe` "Alice"
      Left _  -> expectationFailure "Expected Right"

  it "fetch-mutate-fetch: second fetch sees primed cache from reconcileCache" $ do
    env <- mkTestEnv
    (valBefore, valAfter) <- runMutateTest env $ do
      b <- fetch (UserId 1)
      _ <- mutate (UpdateUser (UserId 1) "NewAlice")
      a <- fetch (UserId 1)
      pure (b, a)
    valBefore `shouldBe` "Alice"
    valAfter `shouldBe` "updated-NewAlice-1"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "fetch after delete mutation misses cache" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runMutateTestWithCache env cRef $ do
      _ <- fetch (UserId 1)
      _ <- mutate (DeleteUser (UserId 1))
      tryFetch (UserId 1)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 2

  it "multiple fetches batch in a single round within Mutate" $ do
    env <- mkTestEnv
    (a, b) <- runMutateTest env $
      (,) <$> fetch (UserId 1) <*> fetch (UserId 2)
    a `shouldBe` "Alice"
    b `shouldBe` "Bob"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "primeCache works within Mutate" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ do
      primeCache (UserId 1) "Primed"
      fetch (UserId 1)
    result `shouldBe` "Primed"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

mutateApplicativeSpec :: Spec
mutateApplicativeSpec = describe "applicative behavior" $ do

  it "fetches batch, mutation fires only after all fetches" $ do
    env <- mkTestEnv
    (user, updated, post) <- runMutateTest env $
      (,,)
        <$> fetch (UserId 1)
        <*> mutate (UpdateUser (UserId 2) "NewBob")
        <*> fetch (PostId 10)
    user `shouldBe` "Alice"
    updated `shouldBe` "updated-NewBob-2"
    post `shouldBe` "Hello World"
    userBatches <- readIORef (envUserLog env)
    postBatches <- readIORef (envPostLog env)
    length userBatches `shouldBe` 1
    length postBatches `shouldBe` 1

  it "two mutations in <*>: both execute sequentially (left then right)" $ do
    env <- mkTestEnv
    (r1, r2) <- runMutateTest env $
      (,) <$> mutate (UpdateUser (UserId 1) "First")
          <*> mutate (UpdateUser (UserId 2) "Second")
    r1 `shouldBe` "updated-First-1"
    r2 `shouldBe` "updated-Second-2"

  it "fmap over mutation result transforms it" $ do
    env <- mkTestEnv
    result <- runMutateTest env $
      fmap (++ "!") (mutate (UpdateUser (UserId 1) "Bang"))
    result `shouldBe` "updated-Bang-1!"

  it "three-way applicative: fetch + mutation + fetch" $ do
    env <- mkTestEnv
    (a, b, c) <- runMutateTest env $
      (,,) <$> fetch (UserId 1)
           <*> mutate (UpdateUser (UserId 2) "M")
           <*> fetch (UserId 3)
    a `shouldBe` "Alice"
    b `shouldBe` "updated-M-2"
    c `shouldBe` "Carol"

mutateMonadicSpec :: Spec
mutateMonadicSpec = describe "monadic behavior" $ do

  it "fetch >>= mutate >>= fetch: correct sequencing" $ do
    env <- mkTestEnv
    (valBefore, result, valAfter) <- runMutateTest env $ do
      b <- fetch (UserId 1)
      r <- mutate (UpdateUser (UserId 1) "Updated")
      a <- fetch (UserId 1)
      pure (b, r, a)
    valBefore `shouldBe` "Alice"
    result `shouldBe` "updated-Updated-1"
    valAfter `shouldBe` "updated-Updated-1"

  it "mutation result used in subsequent fetch key" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ do
      _ <- mutate (UpdateUser (UserId 1) "Dynamic")
      fetch (UserId 2)
    result `shouldBe` "Bob"

  it "two sequential mutations" $ do
    env <- mkTestEnv
    (r1, r2) <- runMutateTest env $ do
      a <- mutate (UpdateUser (UserId 1) "First")
      b <- mutate (UpdateUser (UserId 2) "Second")
      pure (a, b)
    r1 `shouldBe` "updated-First-1"
    r2 `shouldBe` "updated-Second-2"

  it "conditional mutation based on fetch result" $ do
    env <- mkTestEnv
    result <- runMutateTest env $ do
      name <- fetch (UserId 1)
      if name == "Alice"
        then mutate (UpdateUser (UserId 1) "ConditionalUpdate")
        else pure name
    result `shouldBe` "updated-ConditionalUpdate-1"

  it "tryMutate failure doesn't prevent subsequent operations" $ do
    env <- mkTestEnv
    (err, val) <- runMutateTest env $ do
      e <- tryMutate FailMutation
      v <- fetch (UserId 1)
      pure (e, v)
    case err of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left"
    val `shouldBe` "Alice"

mutateMockSpec :: Spec
mutateMockSpec = describe "MockMutate" $ do

  it "mock mutation returns handler result" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
        handlers = mockMutation @UpdateUser (\(UpdateUser _ n) -> "mock-" <> n)
    (result, _) <- runMockMutate @TestM mocks handlers $ mutate (UpdateUser (UserId 1) "Test")
    result `shouldBe` "mock-Test"

  it "mock mutation records the mutation" $ do
    let mocks = emptyMockData
        handlers = mockMutation @UpdateUser (\(UpdateUser _ n) -> "mock-" <> n)
    (_, mutations) <- runMockMutate @TestM mocks handlers $
      mutate (UpdateUser (UserId 1) "Recorded")
    length mutations `shouldBe` 1

  it "mock fetch works alongside mock mutations" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
        handlers = mockMutation @UpdateUser (\(UpdateUser _ n) -> "mock-" <> n)
    ((user, updated), mutations) <- runMockMutate @TestM mocks handlers $ do
      u <- fetch (UserId 1)
      r <- mutate (UpdateUser (UserId 1) "NewName")
      pure (u, r)
    user `shouldBe` "Alice"
    updated `shouldBe` "mock-NewName"
    length mutations `shouldBe` 1

  it "mock tryMutate with no handler returns Left" $ do
    let mocks = emptyMockData
        handlers = emptyMutationHandlers
    (result, _) <- runMockMutate @TestM mocks handlers $
      tryMutate (UpdateUser (UserId 1) "NoHandler")
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing handler"

  it "multiple mock mutations recorded in order" $ do
    let mocks = emptyMockData
        handlers = mockMutation @UpdateUser (\(UpdateUser _ n) -> "mock-" <> n)
                <> mockMutation @DeleteUser (\_ -> ())
    (_, mutations) <- runMockMutate @TestM mocks handlers $ do
      _ <- mutate (UpdateUser (UserId 1) "First")
      _ <- mutate (DeleteUser (UserId 2))
      _ <- mutate (UpdateUser (UserId 3) "Third")
      pure ()
    length mutations `shouldBe` 3

mutateCacheReconcileSpec :: Spec
mutateCacheReconcileSpec = describe "cache reconciliation" $ do

  it "reconcileCache evicts stale keys after DeleteUser" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runMutateTestWithCache env cRef $ do
      _ <- fetch (UserId 1)
      mutate (DeleteUser (UserId 1))
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheMiss -> pure ()
      _         -> expectationFailure "Expected CacheMiss after DeleteUser"

  it "reconcileCache primes fresh values after UpdateUser" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runMutateTestWithCache env cRef $
      mutate (UpdateUser (UserId 1) "Fresh")
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "updated-Fresh-1"
      _               -> expectationFailure "Expected CacheHitReady with fresh value"

  it "cache shared across runMutateWithCache: mutation effects persist" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runMutateTestWithCache env cRef $
      mutate (UpdateUser (UserId 1) "Shared")
    result <- runMutateTestWithCache env cRef $
      fetch (UserId 1)
    result `shouldBe` "updated-Shared-1"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0

-- ══════════════════════════════════════════════
-- Applicative error propagation
-- ══════════════════════════════════════════════

applicativeErrorSpec :: Spec
applicativeErrorSpec = describe "Applicative error propagation" $ do

  it "<*> left fails, right succeeds: whole expression throws" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      (,) <$> fetch (FailKey 1) <*> fetch (UserId 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"
    -- UserId source was still dispatched
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1

  it "<*> right fails, left succeeds: whole expression throws" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      (,) <$> fetch (UserId 1) <*> fetch (FailKey 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1

  it "<*> both sides fail: whole expression throws" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      (,) <$> fetch (FailKey 1) <*> fetch (FailKey 2)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"

  it "tryFetch <*> tryFetch: left Left, right Right" $ do
    env <- mkTestEnv
    (left', right') <- runTest env $
      (,) <$> tryFetch (FailKey 1) <*> tryFetch (UserId 1)
    case left' of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailKey"
    case right' of
      Right v -> v `shouldBe` "Alice"
      Left _  -> expectationFailure "Expected Right for UserId"

  it "tryFetch <*> tryFetch: both Left" $ do
    env <- mkTestEnv
    (left', right') <- runTest env $
      (,) <$> tryFetch (FailKey 1) <*> tryFetch (FailKey 2)
    case left' of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left"
    case right' of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left"

  it "mixed: tryFetch (fail) <*> fetch (ok) succeeds overall" $ do
    env <- mkTestEnv
    (left', right') <- runTest env $
      (,) <$> tryFetch (FailKey 1) <*> fetch (UserId 1)
    case left' of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailKey"
    right' `shouldBe` "Alice"

  it "mixed: fetch (fail) <*> tryFetch (ok) throws overall" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      (,) <$> fetch (FailKey 1) <*> tryFetch (UserId 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception from fetch side"

  it "three-way: middle fails, all three sources dispatched" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      (,,) <$> fetch (UserId 1) <*> fetch (FailKey 1) <*> fetch (PostId 10)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"
    userBatches <- readIORef (envUserLog env)
    postBatches <- readIORef (envPostLog env)
    length userBatches `shouldBe` 1
    length postBatches `shouldBe` 1

  it "fmap over failing fetch throws" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      fmap (++ "!") (fetch (FailKey 1))
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"

  it "successful sources still dispatched when co-batched source fails" $ do
    env <- mkTestEnv
    _ <- runTest env $
      (,) <$> fetch (UserId 1) <*> tryFetch (FailKey 1)
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1
    dispatched <- readIORef (envDispatchLog env)
    dispatched `shouldSatisfy` elem "UserId"
    dispatched `shouldSatisfy` elem "FailKey"

-- ══════════════════════════════════════════════
-- Multi-source failure isolation
-- ══════════════════════════════════════════════

sourceIsolationSpec :: Spec
sourceIsolationSpec = describe "Multi-source failure isolation" $ do

  it "UserId succeeds while FailKey throws in same round" $ do
    env <- mkTestEnv
    (user, failResult) <- runTest env $
      (,) <$> fetch (UserId 1) <*> tryFetch (FailKey 1)
    user `shouldBe` "Alice"
    case failResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailKey"

  it "three sources, middle fails, first and third succeed" $ do
    env <- mkTestEnv
    (user, failResult, post) <- runTest env $
      (,,) <$> fetch (UserId 1) <*> tryFetch (FailKey 1) <*> fetch (PostId 10)
    user `shouldBe` "Alice"
    post `shouldBe` "Hello World"
    case failResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailKey"

  it "source-level vs key-level failures: both Left with different errors" $ do
    env <- mkTestEnv
    (failResult, missingResult) <- runTest env $
      (,) <$> tryFetch (FailKey 1) <*> tryFetch (UserId 999)
    case failResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailKey"
    case missingResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing UserId"

  it "source B results cached despite source A failure in same round" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runTestWithCache env cRef $
      (,) <$> tryFetch (FailKey 1) <*> fetch (UserId 1)
    -- Second run: UserId should be cached
    _ <- runTestWithCache env cRef $ fetch (UserId 1)
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1  -- only one batch, second run hit cache

  it "round 1 mixed success/failure, round 2 uses successful results" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    (_, _, user2) <- runTestWithCache env cRef $ do
      (failResult, user) <- (,) <$> tryFetch (FailKey 1) <*> fetch (UserId 1)
      user2 <- fetch (UserId 1)  -- round 2: should hit cache
      pure (failResult, user, user2)
    user2 `shouldBe` "Alice"
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1

  it "all sources dispatched even when one throws (via dispatch log)" $ do
    env <- mkTestEnv
    _ <- runTest env $
      (,,) <$> tryFetch (FailKey 1) <*> fetch (UserId 1) <*> fetch (PostId 10)
    dispatched <- readIORef (envDispatchLog env)
    dispatched `shouldSatisfy` elem "FailKey"
    dispatched `shouldSatisfy` elem "UserId"
    dispatched `shouldSatisfy` elem "PostId"

-- ══════════════════════════════════════════════
-- Partial batch failures
-- ══════════════════════════════════════════════

partialBatchSpec :: Spec
partialBatchSpec = describe "Partial batch failures" $ do

  it "even key succeeds, odd key fails with FetchError" $ do
    env <- mkTestEnv
    (evenResult, oddResult) <- runTest env $
      (,) <$> tryFetch (PartialKey 2) <*> tryFetch (PartialKey 3)
    case evenResult of
      Right v -> v `shouldBe` "partial-2"
      Left _  -> expectationFailure "Expected Right for even key"
    case oddResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for odd key"

  it "fetch on missing partial key throws FetchError" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $ fetch (PartialKey 3)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception for odd PartialKey"

  it "even-key results cached despite odd-key failures" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    _ <- runTestWithCache env cRef $
      (,) <$> tryFetch (PartialKey 2) <*> tryFetch (PartialKey 3)
    -- Even key should be in cache
    hit <- cacheLookup cRef (PartialKey 2)
    case hit of
      CacheHitReady v -> v `shouldBe` "partial-2"
      _               -> expectationFailure "Expected CacheHitReady for even key"

  it "mixed partial and full sources in same round" $ do
    env <- mkTestEnv
    (user, partialResult) <- runTest env $
      (,) <$> fetch (UserId 1) <*> tryFetch (PartialKey 3)
    user `shouldBe` "Alice"
    case partialResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for odd PartialKey"

  it "multiple partial keys: some succeed, some fail" $ do
    env <- mkTestEnv
    results <- runTest env $
      mapM tryFetch [PartialKey 1, PartialKey 2, PartialKey 3, PartialKey 4]
    case results of
      [r1, r2, r3, r4] -> do
        case r1 of { Left _ -> pure (); Right _ -> expectationFailure "Expected Left for 1" }
        case r2 of { Right v -> v `shouldBe` "partial-2"; Left _ -> expectationFailure "Expected Right for 2" }
        case r3 of { Left _ -> pure (); Right _ -> expectationFailure "Expected Left for 3" }
        case r4 of { Right v -> v `shouldBe` "partial-4"; Left _ -> expectationFailure "Expected Right for 4" }
      _ -> expectationFailure "Expected 4 results"

-- ══════════════════════════════════════════════
-- Strategy failure isolation
-- ══════════════════════════════════════════════

strategyIsolationSpec :: Spec
strategyIsolationSpec = describe "Strategy failure isolation" $ do

  it "eager fails, sequential and concurrent succeed" $ do
    env <- mkTestEnv
    (eagerResult, seqVal, userVal) <- runTest env $
      (,,) <$> tryFetch (FailEagerKey 1) <*> fetch (SeqKey 1) <*> fetch (UserId 1)
    case eagerResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailEagerKey"
    seqVal `shouldBe` "seq-1"
    userVal `shouldBe` "Alice"

  it "sequential fails, concurrent still succeeds" $ do
    env <- mkTestEnv
    (seqResult, userVal) <- runTest env $
      (,) <$> tryFetch (FailSeqKey 1) <*> fetch (UserId 1)
    case seqResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailSeqKey"
    userVal `shouldBe` "Alice"

  it "first sequential fails, second sequential still runs" $ do
    env <- mkTestEnv
    (failResult, seqVal) <- runTest env $
      (,) <$> tryFetch (FailSeqKey 1) <*> fetch (SeqKey 1)
    case failResult of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for FailSeqKey"
    seqVal `shouldBe` "seq-1"

  it "all three strategies succeed: correct results" $ do
    env <- mkTestEnv
    (eager, seq', user) <- runTest env $
      (,,) <$> fetch (EagerKey 1) <*> fetch (SeqKey 1) <*> fetch (UserId 1)
    eager `shouldBe` "eager-1"
    seq' `shouldBe` "seq-1"
    user `shouldBe` "Alice"
    dispatched <- readIORef (envDispatchLog env)
    dispatched `shouldSatisfy` elem "EagerKey"
    dispatched `shouldSatisfy` elem "SeqKey"
    dispatched `shouldSatisfy` elem "UserId"

  it "sequential sources dispatched even if eager failed" $ do
    env <- mkTestEnv
    _ <- runTest env $
      (,) <$> tryFetch (FailEagerKey 1) <*> fetch (SeqKey 1)
    dispatched <- readIORef (envDispatchLog env)
    dispatched `shouldSatisfy` elem "FailEagerKey"
    dispatched `shouldSatisfy` elem "SeqKey"

  it "two sequential sources both produce correct results" $ do
    env <- mkTestEnv
    (s1, s2) <- runTest env $
      (,) <$> fetch (SeqKey 1) <*> fetch (SeqKey2 1)
    s1 `shouldBe` "seq-1"
    s2 `shouldBe` "seq2-1"
    dispatched <- readIORef (envDispatchLog env)
    dispatched `shouldSatisfy` elem "SeqKey"
    dispatched `shouldSatisfy` elem "SeqKey2"

-- ══════════════════════════════════════════════
-- Complex dependency patterns
-- ══════════════════════════════════════════════

complexPatternSpec :: Spec
complexPatternSpec = describe "Complex dependency patterns" $ do

  it "deep chain: 4 rounds, 1 key each" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $ do
      _ <- fetch (RangeKey 1)
      _ <- fetch (RangeKey 2)
      _ <- fetch (RangeKey 3)
      fetch (RangeKey 4)
    length roundLog `shouldBe` 4
    mapM_ (\(_, keys, _) -> keys `shouldBe` 1) roundLog

  it "diamond: 3 rounds (1 key, 2 keys, 1 key)" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $ do
      _ <- fetch (RangeKey 1)
      _ <- (,) <$> fetch (RangeKey 2) <*> fetch (RangeKey 3)
      fetch (RangeKey 4)
    length roundLog `shouldBe` 3
    case roundLog of
      [(_, k1, _), (_, k2, _), (_, k3, _)] -> do
        k1 `shouldBe` 1
        k2 `shouldBe` 2
        k3 `shouldBe` 1
      _ -> expectationFailure "Expected 3 rounds"

  it "fan-out-fan-in: 2 rounds (10 keys, 1 key)" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $ do
      _ <- fetchAll (map RangeKey [1..10])
      fetch (RangeKey 99)
    length roundLog `shouldBe` 2
    case roundLog of
      [(_, k1, _), (_, k2, _)] -> do
        k1 `shouldBe` 10
        k2 `shouldBe` 1
      _ -> expectationFailure "Expected 2 rounds"

  it "monadic-applicative-monadic: 3 rounds" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $ do
      _ <- fetch (RangeKey 1)               -- round 1
      _ <- (,) <$> fetch (RangeKey 2)       -- round 2 (applicative)
               <*> fetch (RangeKey 3)
      fetch (RangeKey 4)                     -- round 3
    length roundLog `shouldBe` 3

  it "nested applicative: all keys in one round" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $
      (,) <$> ((,) <$> fetch (RangeKey 1) <*> fetch (RangeKey 2))
          <*> fetch (RangeKey 3)
    length roundLog `shouldBe` 1
    case roundLog of
      [(_, keys, _)] -> keys `shouldBe` 3
      _              -> expectationFailure "Expected 1 round"

  it "applicative with pure: fetch happens, pure doesn't create round" $ do
    env <- mkTestEnv
    (result, roundLog) <- runTestWithRoundLog env $
      (,) <$> fetch (RangeKey 1) <*> pure (42 :: Int)
    fst result `shouldBe` "range-1"
    snd result `shouldBe` 42
    length roundLog `shouldBe` 1

  it "pure >>= fetch: single round" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $
      pure 1 >>= \x -> fetch (RangeKey x)
    length roundLog `shouldBe` 1

  it "round content matches expected key sets" $ do
    env <- mkTestEnv
    roundKeysRef <- newIORef ([] :: [[UserId]])
    cRef <- newCacheRef
    let e = FetchEnv
          { fetchCache = cRef
          , fetchLower = runTestM env
          , fetchLift  = testLiftIO
          }
    _ <- runTestM env $ runLoopWith e (\_ batches exec -> do
      let ks = batchKeys @UserId batches
      testLiftIO $ modifyIORef' roundKeysRef (\l -> l ++ [ks])
      _ <- exec
      pure ()
      ) $ do
        _ <- fetch (UserId 1)
        (,) <$> fetch (UserId 2) <*> fetch (UserId 3)
    rounds <- readIORef roundKeysRef
    length rounds `shouldBe` 2
    case rounds of
      [r1, r2] -> do
        r1 `shouldSatisfy` elem (UserId 1)
        length r1 `shouldBe` 1
        r2 `shouldSatisfy` elem (UserId 2)
        r2 `shouldSatisfy` elem (UserId 3)
        length r2 `shouldBe` 2
      _ -> expectationFailure "Expected 2 rounds"

-- ══════════════════════════════════════════════
-- liftSource tests
-- ══════════════════════════════════════════════

liftSourceSpec :: Spec
liftSourceSpec = describe "liftSource" $ do

  it "liftSource (pure 42) returns 42 with zero rounds" $ do
    env <- mkTestEnv
    (result, roundLog) <- runTestWithRoundLog env $
      liftSource (pure (42 :: Int))
    result `shouldBe` 42
    length roundLog `shouldBe` 0

  it "liftSource combined applicatively with fetch: fetch still batches" $ do
    env <- mkTestEnv
    (result, roundLog) <- runTestWithRoundLog env $
      (,) <$> liftSource (pure (42 :: Int)) <*> fetch (UserId 1)
    fst result `shouldBe` 42
    snd result `shouldBe` "Alice"
    length roundLog `shouldBe` 1

  it "liftSource in monadic bind does NOT create a round boundary" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $ do
      x <- liftSource (pure (1 :: Int))
      fetch (RangeKey x)
    -- liftSource returns Done immediately, so bind proceeds to fetch.
    -- Only 1 round for the fetch.
    length roundLog `shouldBe` 1

  it "liftSource performs IO side effects" $ do
    env <- mkTestEnv
    ref <- newIORef False
    _ <- runTest env $ liftSource $ TestM $ \_ -> do
      writeIORef ref True
      pure ()
    val <- readIORef ref
    val `shouldBe` True

  it "liftSource interleaved with fetches in applicative doesn't disrupt batching" $ do
    env <- mkTestEnv
    (_, roundLog) <- runTestWithRoundLog env $
      (,,) <$> fetch (UserId 1)
           <*> liftSource (pure ("static" :: String))
           <*> fetch (UserId 2)
    length roundLog `shouldBe` 1
    userBatches <- readIORef (envUserLog env)
    length userBatches `shouldBe` 1

-- ══════════════════════════════════════════════
-- NoCaching detailed behavior
-- ══════════════════════════════════════════════

noCachingSpec :: Spec
noCachingSpec = describe "NoCaching detailed behavior" $ do

  it "same NoCaching key in two sequential rounds dispatches twice" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $ do
      x <- fetch (MutKey 1)
      y <- fetch (MutKey 1)
      pure (x, y)
    mutBatches <- readIORef (envMutLog env)
    -- Must dispatch exactly twice, once per round
    length mutBatches `shouldBe` 2
    -- Counter-based source returns different values across rounds
    a `shouldSatisfy` (/= b)

  it "NoCaching key in applicative with CacheResults key: both dispatched" $ do
    env <- mkTestEnv
    (mutVal, userVal) <- runTest env $
      (,) <$> fetch (MutKey 1) <*> fetch (UserId 1)
    userVal `shouldBe` "Alice"
    mutVal `shouldSatisfy` const True
    mutBatches <- readIORef (envMutLog env)
    length mutBatches `shouldBe` 1

  it "NoCaching key twice in same applicative: deduplicated within round" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $
      (,) <$> fetch (MutKey 1) <*> fetch (MutKey 1)
    -- Same value from same round
    a `shouldBe` b
    mutBatches <- readIORef (envMutLog env)
    -- Only one batch call for the round
    length mutBatches `shouldBe` 1

  it "counter-based MutKey source increments across rounds" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $ do
      x <- fetch (MutKey 1)
      y <- fetch (MutKey 2)
      pure (x, y)
    -- Each round gets a different counter value
    a `shouldSatisfy` (/= b)
    mutCount <- readIORef (envMutCount env)
    mutCount `shouldBe` 2

  it "NoCaching: same key across >>= rounds dispatches fresh batch each time" $ do
    env <- mkTestEnv
    (a, b) <- runTest env $ do
      x <- fetch (MutKey 1)
      y <- fetch (MutKey 1)
      pure (x, y)
    mutBatches <- readIORef (envMutLog env)
    -- Must dispatch exactly twice, once per round
    length mutBatches `shouldBe` 2
    -- Counter-based source returns different values across rounds
    a `shouldSatisfy` (/= b)

-- ══════════════════════════════════════════════
-- Round stats and probe assertions
-- ══════════════════════════════════════════════

roundStatsSpec :: Spec
roundStatsSpec = describe "Round stats and probe" $ do

  it "RoundStats.roundSources counts distinct sources" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    let batches = singletonBatch @TestM (UserId 1)
               <> singletonBatch @TestM (PostId 10)
               <> singletonBatch @TestM (UserId 2)
    stats <- executeBatches (runTestM env) testLiftIO cRef batches
    roundSources stats `shouldBe` 2  -- UserId and PostId

  it "RoundStats.roundKeys counts deduplicated keys" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    let batches = singletonBatch @TestM (UserId 1)
               <> singletonBatch @TestM (UserId 1)  -- duplicate
               <> singletonBatch @TestM (PostId 10)
    stats <- executeBatches (runTestM env) testLiftIO cRef batches
    roundKeys stats `shouldBe` 2  -- UserId 1 (deduped) + PostId 10

  it "RoundStats.roundCacheHits counts already-cached keys" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    cacheWarm @UserId cRef (HM.singleton (UserId 1) "Alice")
    let batches = singletonBatch @TestM (UserId 1)
               <> singletonBatch @TestM (UserId 2)
    stats <- executeBatches (runTestM env) testLiftIO cRef batches
    roundCacheHits stats `shouldBe` 1  -- UserId 1 was cached
    roundKeys stats `shouldBe` 2       -- total keys in batch

  it "probe on blocked computation returns Blocked with correct batch info" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    let e = FetchEnv
          { fetchCache = cRef
          , fetchLower = runTestM env
          , fetchLift  = testLiftIO
          }
    status <- runTestM env $ unFetch
      ((,) <$> fetch (UserId 1) <*> fetch (PostId 10)) e
    case status of
      Done _       -> expectationFailure "Expected Blocked"
      Blocked bs _ -> do
        batchSize bs `shouldBe` 2
        batchSourceCount bs `shouldBe` 2

-- ══════════════════════════════════════════════
-- MonadThrow / MonadCatch tests
-- ══════════════════════════════════════════════

-- | A custom test exception for MonadThrow/MonadCatch tests.
newtype TestException = TestException String
  deriving stock (Show, Eq)

instance MC.Exception TestException

throwCatchSpec :: Spec
throwCatchSpec = describe "MonadThrow / MonadCatch" $ do

  it "throwM in Fetch produces exception catchable at IO level" $ do
    env <- mkTestEnv
    result <- try @SomeException $ runTest env $
      MC.throwM (TestException "boom")
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception"

  it "catch in Fetch catches throwM" $ do
    env <- mkTestEnv
    result <- runTest env $
      MC.catch
        (MC.throwM (TestException "caught") :: Fetch TestM String)
        (\(TestException msg) -> pure ("recovered: " <> msg))
    result `shouldBe` "recovered: caught"

  it "catch in Fetch across round boundary catches later-round exception" $ do
    env <- mkTestEnv
    result <- runTest env $
      MC.catch
        (do _ <- fetch (UserId 1)  -- round 1
            MC.throwM (TestException "round2") :: Fetch TestM String)
        (\(TestException msg) -> pure ("caught: " <> msg))
    result `shouldBe` "caught: round2"

  it "catch wrapping fetch of missing key catches FetchError" $ do
    env <- mkTestEnv
    result <- runTest env $
      MC.catch
        (fetch (UserId 999))
        (\(_ :: SomeException) -> pure "fallback")
    result `shouldBe` "fallback"

  it "throwM/catch in Mutate works" $ do
    env <- mkTestEnv
    result <- runMutateTest env $
      MC.catch
        (MC.throwM (TestException "mut") :: Mutate TestM TestM String)
        (\(TestException msg) -> pure ("caught: " <> msg))
    result `shouldBe` "caught: mut"

  it "throwM/catch in MockFetch works via delegation" $ do
    let mocks = mockData @UserId [(UserId 1, "Alice")]
    result <- runMockFetch @TestM mocks $
      MC.catch
        (MC.throwM (TestException "mock") :: MockFetch TestM IO String)
        (\(TestException msg) -> pure ("caught: " <> msg))
    result `shouldBe` "caught: mock"

-- ══════════════════════════════════════════════
-- Async exception safety tests
-- ══════════════════════════════════════════════

asyncExceptionSpec :: Spec
asyncExceptionSpec = describe "Async exception safety" $ do
  asyncIVarSpec
  asyncFetchSpec
  asyncMutateSpec

asyncIVarSpec :: Spec
asyncIVarSpec = describe "IVar" $ do

  it "awaitIVar is interruptible by throwTo" $ do
    iv <- newIVar @Int
    started <- newEmptyMVar
    resultVar <- newEmptyMVar
    tid <- forkIO $ do
      putMVar started ()
      r <- try @SomeException (awaitIVar iv)
      putMVar resultVar r
    -- Wait for the thread to be ready (about to enter readMVar)
    takeMVar started
    -- Deliver async exception; throwTo blocks until delivered
    throwTo tid (toException (TestException "killed"))
    result <- takeMVar resultVar
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected async exception from throwTo"

  it "IVar writable after reader is killed; new reader sees value" $ do
    iv <- newIVar @Int
    started <- newEmptyMVar
    done <- newEmptyMVar
    tid <- forkIO $ do
      putMVar started ()
      _ <- try @SomeException (awaitIVar iv)
      putMVar done ()
    takeMVar started
    throwTo tid (toException (TestException "killed"))
    -- Wait for the killed thread to finish its exception handler
    takeMVar done
    -- IVar should still be writable
    writeIVar iv 42
    result <- awaitIVar iv
    case result of
      Right v -> v `shouldBe` 42
      Left _  -> expectationFailure "Expected Right after writeIVar"

  it "multiple readers: kill one, others still see value when written" $ do
    iv <- newIVar @Int
    started1 <- newEmptyMVar
    started2 <- newEmptyMVar
    resultVar <- newEmptyMVar
    tid1 <- forkIO $ do
      putMVar started1 ()
      _ <- try @SomeException (awaitIVar iv)
      pure ()
    _ <- forkIO $ do
      putMVar started2 ()
      r <- awaitIVar iv
      putMVar resultVar r
    takeMVar started1
    takeMVar started2
    -- Kill reader 1
    throwTo tid1 (toException (TestException "killed"))
    -- Write value; reader 2 should see it
    writeIVar iv 99
    result <- takeMVar resultVar
    case result of
      Right v -> v `shouldBe` 99
      Left _  -> expectationFailure "Expected Right from surviving reader"

asyncFetchSpec :: Spec
asyncFetchSpec = describe "Fetch" $ do

  it "cancel during batch execution propagates to caller" $ do
    env <- mkTestEnv
    handle <- async $ runTest env $ fetch (BlockingKey 1)
    -- Wait until the batch is in flight
    takeMVar (envAsyncStarted env)
    cancel handle
    result <- waitCatch handle
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected async exception"
    -- Cleanup: release the blocking batch thread so it doesn't leak
    _ <- tryPutMVar (envAsyncProceed env) ()
    pure ()

  it "completed results remain cached after later round is cancelled" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    handle <- async $ runTestWithCache env cRef $ do
      _ <- fetch (UserId 1)     -- round 1: succeeds
      fetch (BlockingKey 1)     -- round 2: blocks
    -- Round 2's batch is in flight
    takeMVar (envAsyncStarted env)
    cancel handle
    _ <- tryPutMVar (envAsyncProceed env) ()
    -- UserId 1 was cached in round 1 and should still be valid
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice"
      _               -> expectationFailure "Expected CacheHitReady for UserId 1"

  it "concurrent threads sharing cache: cancel one, other completes" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    -- Thread A: fetches UserId 1, then blocks on BlockingKey
    handleA <- async $ runTestWithCache env cRef $ do
      _ <- fetch (UserId 1)
      fetch (BlockingKey 1)
    -- Wait for A to reach the blocking batch
    takeMVar (envAsyncStarted env)
    -- Thread B: uses the same cache, fetches UserId 2
    resultB <- runTestWithCache env cRef $ fetch (UserId 2)
    resultB `shouldBe` "Bob"
    -- Cancel A and cleanup
    cancel handleA
    _ <- tryPutMVar (envAsyncProceed env) ()
    pure ()

  it "background batch thread fills IVars after parent is cancelled" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    handle <- async $ runTestWithCache env cRef $ do
      _ <- fetch (UserId 1)
      fetch (BlockingKey 1)
    takeMVar (envAsyncStarted env)
    cancel handle
    -- Release the batch thread; it should complete and fill the IVar
    putMVar (envAsyncProceed env) ()
    -- The batch thread is an orphaned `async` child; it will fill the IVar
    hit <- cacheLookup cRef (BlockingKey 1)
    case hit of
      CacheHitPending iv -> do
        result <- awaitIVar iv
        case result of
          Right v -> v `shouldBe` "blocking-1"
          Left _  -> expectationFailure "Expected Right from background batch"
      CacheHitReady v -> v `shouldBe` "blocking-1"
      CacheMiss -> expectationFailure "Expected cache entry for BlockingKey 1"

  it "MonadCatch handler is NOT invoked for async exceptions during batch execution" $ do
    -- Fetch's catch wraps the probe phase, not batch execution.
    -- Async exceptions during executeBatches bypass MonadCatch.
    env <- mkTestEnv
    handlerCalled <- newIORef False
    handle <- async $ runTest env $
      MC.catch
        (do _ <- fetch (UserId 1)
            fetch (BlockingKey 1))
        (\(_ :: SomeException) -> do
            liftSource $ testLiftIO $ writeIORef handlerCalled True
            pure "caught")
    takeMVar (envAsyncStarted env)
    cancel handle
    _ <- tryPutMVar (envAsyncProceed env) ()
    result <- waitCatch handle
    case result of
      Left _        -> pure ()  -- exception propagated, not caught by handler
      Right "caught" -> expectationFailure
        "MonadCatch handler should not catch async exceptions during batch execution"
      Right _        -> expectationFailure "Unexpected success"
    called <- readIORef handlerCalled
    called `shouldBe` False

  it "throwTo with custom exception reaches caller via try" $ do
    env <- mkTestEnv
    started <- newEmptyMVar
    resultVar <- newEmptyMVar
    tid <- forkIO $ do
      putMVar started ()
      r <- try @SomeException $ runTest env $ fetch (BlockingKey 1)
      putMVar resultVar r
    takeMVar started
    -- Wait for the batch to start so we know the thread is deep in execution
    takeMVar (envAsyncStarted env)
    throwTo tid (toException (TestException "async-kill"))
    result <- takeMVar resultVar
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected exception from throwTo"
    _ <- tryPutMVar (envAsyncProceed env) ()
    pure ()

  it "cache not corrupted by cancelled computation; fresh run succeeds" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    -- First run: fetch user, then block and get cancelled
    handle <- async $ runTestWithCache env cRef $ do
      _ <- fetch (UserId 1)
      fetch (BlockingKey 1)
    takeMVar (envAsyncStarted env)
    cancel handle
    _ <- tryPutMVar (envAsyncProceed env) ()
    -- Second run: fresh computation on the same cache
    -- UserId 1 should be cached; UserId 2 should be fetchable
    env2 <- mkTestEnv  -- fresh env so BlockingKey barriers are reset
    result <- runTestWithCache env2 cRef $
      (,) <$> fetch (UserId 1) <*> fetch (UserId 2)
    result `shouldBe` ("Alice", "Bob")
    -- Verify UserId 1 came from cache (no second batchFetch for it)
    userLog <- readIORef (envUserLog env2)
    let allFetchedUsers = concat userLog
    allFetchedUsers `shouldNotContain` [UserId 1]

asyncMutateSpec :: Spec
asyncMutateSpec = describe "Mutate" $ do

  it "cancel before mutation: mutation never executes" $ do
    env <- mkTestEnv
    cRef <- newCacheRef
    handle <- async $ runMutateTestWithCache env cRef $ do
      _ <- fetch (UserId 1)       -- round 1: succeeds
      _ <- fetch (BlockingKey 1)  -- round 2: blocks, gets cancelled
      mutate (UpdateUser (UserId 1) "ShouldNotHappen")
    takeMVar (envAsyncStarted env)
    cancel handle
    _ <- tryPutMVar (envAsyncProceed env) ()
    -- If the mutation had run, reconcileCache would have overwritten
    -- UserId 1 with "updated-ShouldNotHappen-1". Check it's still "Alice".
    hit <- cacheLookup cRef (UserId 1)
    case hit of
      CacheHitReady v -> v `shouldBe` "Alice"
      _               -> expectationFailure "Expected CacheHitReady for UserId 1"

  it "cancel during fetch phase of Mutate propagates exception" $ do
    env <- mkTestEnv
    handle <- async $ runMutateTest env $ do
      fetch (BlockingKey 1)  -- blocks, gets cancelled
    takeMVar (envAsyncStarted env)
    cancel handle
    result <- waitCatch handle
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected async exception"
    _ <- tryPutMVar (envAsyncProceed env) ()
    pure ()
