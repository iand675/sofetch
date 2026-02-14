{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Main (main) where

import Fetch
import Fetch.OpenTelemetry

import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import qualified Data.List.NonEmpty as NE
import Data.Maybe (mapMaybe)
import GHC.Generics (Generic)
import OpenTelemetry.Trace.Core
  ( Tracer, makeTracer, getGlobalTracerProvider
  , InstrumentationLibrary(..), tracerOptions
  )
import OpenTelemetry.Attributes (emptyAttributes)
import Test.Hspec

-- ══════════════════════════════════════════════
-- Test key types
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

newtype FailKey = FailKey Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey FailKey where
  type Result FailKey = String

-- ══════════════════════════════════════════════
-- Test monad & data sources
-- ══════════════════════════════════════════════

data TestEnv = TestEnv
  { envUsers   :: HashMap UserId String
  , envUserLog :: IORef [[UserId]]
  , envPosts   :: HashMap PostId String
  }

mkTestEnv :: IO TestEnv
mkTestEnv = TestEnv
  <$> pure (HM.fromList [(UserId 1, "Alice"), (UserId 2, "Bob")])
  <*> newIORef []
  <*> pure (HM.fromList [(PostId 10, "Hello World")])

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

testLiftIO :: IO a -> TestM a
testLiftIO io = TestM $ \_ -> io

runTestM :: TestEnv -> TestM a -> IO a
runTestM env (TestM f) = f env

instance DataSource TestM UserId where
  batchFetch keysNE = do
    let keys = NE.toList keysNE
    env <- askTestEnv
    testLiftIO $ modifyIORef' (envUserLog env) (keys :)
    pure $ HM.fromList
      (mapMaybe (\k -> fmap (\v -> (k, v)) (HM.lookup k (envUsers env))) keys)

instance DataSource TestM PostId where
  batchFetch keysNE = do
    let keys = NE.toList keysNE
    env <- askTestEnv
    pure $ HM.fromList
      (mapMaybe (\k -> fmap (\v -> (k, v)) (HM.lookup k (envPosts env))) keys)

instance DataSource TestM FailKey where
  batchFetch _ = error "FailKey data source exploded"

-- ══════════════════════════════════════════════
-- Helpers
-- ══════════════════════════════════════════════

mkNoopTracer :: IO Tracer
mkNoopTracer = do
  tp <- getGlobalTracerProvider
  pure $ makeTracer tp
    (InstrumentationLibrary "sofetch-otel-test" "" "" emptyAttributes)
    tracerOptions

-- ══════════════════════════════════════════════
-- Tests
-- ══════════════════════════════════════════════

main :: IO ()
main = hspec $ describe "Fetch.OpenTelemetry" $ do

  it "single fetch returns correct value" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    result <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $ fetch (UserId 1)
    result `shouldBe` "Alice"

  it "applicative batching works" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    (a, b) <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $
        (,) <$> fetch (UserId 1) <*> fetch (UserId 2)
    a `shouldBe` "Alice"
    b `shouldBe` "Bob"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "monadic >>= creates separate rounds" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    _ <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $ do
        _ <- fetch (UserId 1)
        fetch (UserId 2)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 2

  it "multi-source batching works" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    (user, post) <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $
        (,) <$> fetch (UserId 1) <*> fetch (PostId 10)
    user `shouldBe` "Alice"
    post `shouldBe` "Hello World"

  it "tryFetch returns Left for missing key" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    result <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $ tryFetch (UserId 999)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for missing key"

  it "data source exceptions are caught by tryFetch" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    result <- runTestM env $
      runFetchTWithOTel tracer (runTestM env) testLiftIO $ tryFetch (FailKey 1)
    case result of
      Left _  -> pure ()
      Right _ -> expectationFailure "Expected Left for failed source"

  it "runFetchTWithCacheWithOTel shares cache across runs" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    cRef <- newCacheRef
    _ <- runTestM env $
      runFetchTWithCacheWithOTel tracer (runTestM env) testLiftIO cRef $ fetch (UserId 1)
    _ <- runTestM env $
      runFetchTWithCacheWithOTel tracer (runTestM env) testLiftIO cRef $ fetch (UserId 1)
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 1

  it "primeCache seeds cache through OTel runner" $ do
    env <- mkTestEnv
    tracer <- mkNoopTracer
    cRef <- newCacheRef
    _ <- runTestM env $
      runFetchTWithCacheWithOTel tracer (runTestM env) testLiftIO cRef $ do
        primeCache (UserId 1) "OTel-Seeded"
    result <- runTestM env $
      runFetchTWithCacheWithOTel tracer (runTestM env) testLiftIO cRef $ fetch (UserId 1)
    result `shouldBe` "OTel-Seeded"
    batches <- readIORef (envUserLog env)
    length batches `shouldBe` 0
