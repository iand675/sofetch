{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

-- | Concurrent exploration of the GitHub REST API.
--
-- This example demonstrates sofetch with an HTTP backend where the
-- value is concurrency, deduplication, and caching, not SQL batching.
--
-- Run it with:
--
-- @
-- stack run github-explorer
-- @
--
-- __Rate limit note:__ GitHub allows 60 unauthenticated requests per
-- hour. This example uses ~15 requests against a handful of stable
-- accounts. To raise the limit to 5,000/hour, set:
--
-- @
-- export GITHUB_TOKEN=ghp_your_token_here
-- @
module Main (main) where

import Fetch

import Control.Exception (SomeException)
import Control.Monad (when)
import Data.Aeson ((.:), (.:?), withObject, FromJSON(..), eitherDecode')
import Data.ByteString.Lazy (ByteString)
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import GHC.Generics (Generic)
import Network.HTTP.Client
  ( Manager
  , newManager, parseRequest, httpLbs, responseBody, responseStatus
  , requestHeaders
  )
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Status (statusCode)
import System.Environment (lookupEnv)

-- ══════════════════════════════════════════════
-- Domain types (parsed from GitHub JSON)
-- ══════════════════════════════════════════════

data GitHubUser = GitHubUser
  { ghUserLogin      :: !Text
  , ghUserName       :: !(Maybe Text)
  , ghUserPublicRepos :: !Int
  , ghUserFollowers  :: !Int
  } deriving (Show, Eq, Generic)

instance FromJSON GitHubUser where
  parseJSON = withObject "GitHubUser" $ \o -> GitHubUser
    <$> o .: "login"
    <*> o .:? "name"
    <*> o .: "public_repos"
    <*> o .: "followers"

data GitHubRepo = GitHubRepo
  { ghRepoName       :: !Text
  , ghRepoStars      :: !Int
  , ghRepoLanguage   :: !(Maybe Text)
  , ghRepoFork       :: !Bool
  } deriving (Show, Eq, Generic)

instance FromJSON GitHubRepo where
  parseJSON = withObject "GitHubRepo" $ \o -> GitHubRepo
    <$> o .: "name"
    <*> o .: "stargazers_count"
    <*> o .:? "language"
    <*> o .: "fork"

-- ══════════════════════════════════════════════
-- FetchKey types
-- ══════════════════════════════════════════════

-- | Fetch a GitHub user profile by login name.
newtype UserLogin = UserLogin Text
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserLogin where
  type Result UserLogin = GitHubUser

-- | Fetch the public repos for a GitHub user.
newtype UserRepos = UserRepos Text
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserRepos where
  type Result UserRepos = [GitHubRepo]

-- ══════════════════════════════════════════════
-- Application monad
-- ══════════════════════════════════════════════

-- | Environment: HTTP manager + optional auth token.
data AppEnv = AppEnv
  { envManager :: !Manager
  , envToken   :: !(Maybe Text)
  }

newtype AppM a = AppM { unAppM :: AppEnv -> IO a }

instance Functor AppM where
  fmap f (AppM g) = AppM $ \e -> fmap f (g e)

instance Applicative AppM where
  pure a = AppM $ \_ -> pure a
  AppM ff <*> AppM fx = AppM $ \e -> ff e <*> fx e

instance Monad AppM where
  AppM ma >>= f = AppM $ \e -> do
    a <- ma e
    unAppM (f a) e

askEnv :: AppM AppEnv
askEnv = AppM pure

liftIO :: IO a -> AppM a
liftIO io = AppM $ \_ -> io

runAppM :: AppEnv -> AppM a -> IO a
runAppM env (AppM f) = f env

-- ══════════════════════════════════════════════
-- HTTP helpers
-- ══════════════════════════════════════════════

-- | Make an authenticated GET request to the GitHub API.
-- Returns Nothing on non-200 responses (so per-key errors don't
-- take down the entire batch).
githubGet :: String -> AppM (Maybe ByteString)
githubGet url = do
  env <- askEnv
  req <- Main.liftIO $ parseRequest url
  let authHeaders = case envToken env of
        Just tok -> [("Authorization", "token " <> TE.encodeUtf8 tok)]
        Nothing  -> []
      req' = req
        { requestHeaders =
            ("User-Agent", "sofetch-example/0.1")
          : ("Accept", "application/vnd.github.v3+json")
          : authHeaders ++ requestHeaders req
        }
  Main.liftIO $ putStrLn $ "  [HTTP] GET " <> url
  resp <- Main.liftIO $ httpLbs req' (envManager env)
  let code = statusCode (responseStatus resp)
  if code == 200
    then pure (Just (responseBody resp))
    else do
      Main.liftIO $ putStrLn $ "  [HTTP] " <> show code <> " " <> url
      pure Nothing

-- ══════════════════════════════════════════════
-- DataSource instances
-- ══════════════════════════════════════════════

-- | Each UserLogin key fires a separate HTTP request, but sofetch
-- runs them concurrently (default FetchStrategy = Concurrent) and
-- deduplicates across the computation.
--
-- Uses 'optionalBatchFetch': keys whose HTTP request fails are
-- omitted from the result map. The engine fills them with an error
-- so that 'tryFetch' returns @Left@ and 'fetch' throws, but
-- successful keys are unaffected.
instance DataSource AppM UserLogin where
  batchFetch = optionalBatchFetch $ \(UserLogin login) -> do
    mbs <- githubGet $ "https://api.github.com/users/" <> T.unpack login
    case mbs of
      Nothing -> pure Nothing
      Just bs -> case eitherDecode' bs of
        Right a  -> pure (Just a)
        Left err -> do
          Main.liftIO $ putStrLn $ "  [JSON] decode error: " <> err
          pure Nothing

instance DataSource AppM UserRepos where
  batchFetch = optionalBatchFetch $ \(UserRepos login) -> do
    mbs <- githubGet $ "https://api.github.com/users/" <> T.unpack login <> "/repos?per_page=5&sort=stars"
    case mbs of
      Nothing -> pure Nothing
      Just bs -> case eitherDecode' bs of
        Right a  -> pure (Just a)
        Left err -> do
          Main.liftIO $ putStrLn $ "  [JSON] decode error: " <> err
          pure Nothing

-- ══════════════════════════════════════════════
-- Instrumented runner
-- ══════════════════════════════════════════════

-- | Run a Fetch computation with detailed round-by-round logging.
runFetchIO :: AppEnv -> Fetch AppM a -> IO a
runFetchIO env action = do
  cRef <- newCacheRef
  totalRoundsRef <- newIORef (0 :: Int)
  totalKeysRef   <- newIORef (0 :: Int)
  totalHitsRef   <- newIORef (0 :: Int)

  let e = FetchEnv
        { fetchCache = cRef
        , fetchLower = runAppM env
        , fetchLift  = Main.liftIO
        }

      withRound n batches exec = do
        let pending  = batchSize batches
            sources  = batchSourceCount batches
        Main.liftIO $ putStrLn $ "  ┌─ Round " <> show n
          <> ": " <> show pending <> " key(s) across "
          <> show sources <> " source(s)"

        stats <- exec

        let dispatched = roundKeys stats - roundCacheHits stats
        Main.liftIO $ do
          when (roundCacheHits stats > 0) $
            putStrLn $ "  │  Cache: " <> show (roundCacheHits stats) <> " hit(s)"
          putStrLn $ "  │  Dispatched: " <> show dispatched <> " key(s) to data sources"
          putStrLn $ "  └─ Round " <> show n <> " complete"
          modifyIORef' totalRoundsRef (+ 1)
          modifyIORef' totalKeysRef (+ roundKeys stats)
          modifyIORef' totalHitsRef (+ roundCacheHits stats)

  a <- runAppM env $ runLoopWith e withRound action

  rounds <- readIORef totalRoundsRef
  keys   <- readIORef totalKeysRef
  hits   <- readIORef totalHitsRef
  putStrLn $ "  ── Summary: " <> show rounds <> " round(s), "
    <> show keys <> " key(s), "
    <> show hits <> " cache hit(s)"
  pure a

-- ══════════════════════════════════════════════
-- Scenarios
-- ══════════════════════════════════════════════

header :: String -> String -> IO ()
header num desc = do
  putStrLn ""
  putStrLn $ "━━━ Scenario " <> num <> ": " <> desc <> " ━━━"

showUser :: GitHubUser -> String
showUser u = T.unpack (ghUserLogin u)
  <> " (" <> maybe "?" T.unpack (ghUserName u)
  <> ", " <> show (ghUserPublicRepos u) <> " repos"
  <> ", " <> show (ghUserFollowers u) <> " followers)"

main :: IO ()
main = do
  mgr <- newManager tlsManagerSettings
  mToken <- lookupEnv "GITHUB_TOKEN"
  let env = AppEnv mgr (T.pack <$> mToken)

  putStrLn "sofetch GitHub Explorer"
  putStrLn $ "Auth: " <> maybe "none (60 req/hour limit)" (const "token (5000 req/hour)") mToken

  -- ── Scenario 1: Concurrent fetches ────────────
  header "1" "Concurrent fetches"
  putStrLn "Fetching two users in parallel (one round, two concurrent HTTP requests)."
  (u1, u2) <- runFetchIO env $
    (,) <$> fetch (UserLogin "haskell") <*> fetch (UserLogin "rust-lang")
  putStrLn $ "  => " <> showUser u1
  putStrLn $ "  => " <> showUser u2

  -- ── Scenario 2: Monadic chain ─────────────────
  header "2" "Monadic chain (2 rounds)"
  putStrLn "Fetching a user, then their repos (data dependency forces 2 rounds)."
  (user, repos) <- runFetchIO env $ do
    u <- fetch (UserLogin "haskell")                 -- round 1
    rs <- fetch (UserRepos (ghUserLogin u))          -- round 2
    pure (u, rs)
  putStrLn $ "  => " <> showUser user
  putStrLn $ "  => Top repos: " <> show (map ghRepoName (take 3 repos))

  -- ── Scenario 3: Fan-out ───────────────────────
  header "3" "Fan-out (fetch many, then fan out)"
  putStrLn "Fetching 3 users in round 1, then all their repos in round 2."
  let logins = [UserLogin "haskell", UserLogin "rust-lang", UserLogin "golang"]
  allRepos <- runFetchIO env $ do
    users <- fetchAll logins                                    -- round 1: 3 concurrent HTTP
    fetchAll (map (UserRepos . ghUserLogin) users)              -- round 2: 3 concurrent HTTP
  putStrLn $ "  => Repo counts: " <> show (map length allRepos)

  -- ── Scenario 4: Deduplication + caching ───────
  header "4" "Deduplication + caching"
  putStrLn "Fetching 'haskell' from TWO code paths. Only ONE HTTP request fires."
  putStrLn "Then fetching 'haskell' again in a later round: cache hit, no HTTP."
  result <- runFetchIO env $ do
    -- Both of these want UserLogin "haskell"; deduplicated into one request
    (a, b) <- (,) <$> fetch (UserLogin "haskell") <*> fetch (UserLogin "haskell")
    -- This monadic bind forces a new round, but the cache has the value
    _ <- fetch (UserLogin "haskell")  -- cache hit
    pure (a, b)
  putStrLn $ "  => Got same user twice: " <> show (fst result == snd result)

  -- ── Scenario 5: Error handling ────────────────
  header "5" "Error handling (tryFetch)"
  putStrLn "Fetching a nonexistent user alongside a real one."
  (good, bad) <- runFetchIO env $
    (,) <$> tryFetch (UserLogin "haskell")
        <*> tryFetch (UserLogin "this-user-definitely-does-not-exist-404-sofetch")
  case good of
    Right u -> putStrLn $ "  => Good: " <> showUser u
    Left  e -> putStrLn $ "  => Good failed: " <> show e
  case bad of
    Right u -> putStrLn $ "  => Bad: " <> showUser u
    Left  e -> putStrLn $ "  => Bad (expected): " <> show (e :: SomeException)

  -- ── Scenario 6: Combinators ───────────────────
  header "6" "Combinators (fetchThrough, fetchMap)"
  putStrLn "Using fetchThrough to pair logins with user profiles:"
  let loginTexts = ["haskell", "rust-lang", "golang"] :: [Text]
  paired <- runFetchIO env $
    fetchThrough UserLogin loginTexts
  mapM_ (\(login, u) -> putStrLn $ "  => " <> T.unpack login
    <> " -> " <> maybe "?" T.unpack (ghUserName u)) paired

  putStrLn ""
  putStrLn "Using fetchMap to extract just follower counts:"
  counts <- runFetchIO env $
    fetchMap UserLogin (\login u -> (login, ghUserFollowers u)) loginTexts
  mapM_ (\(login, n) -> putStrLn $ "  => " <> T.unpack login
    <> ": " <> show n <> " followers") counts

  putStrLn ""
  putStrLn "Done!"
