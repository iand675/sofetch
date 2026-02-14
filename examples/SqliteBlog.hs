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

-- | A blog platform backed by an in-memory SQLite database.
--
-- This example demonstrates how sofetch turns N+1 query cascades into
-- batched SQL. Run it with:
--
-- @
-- stack run sqlite-blog
-- @
--
-- Each scenario prints the SQL queries that are executed, so you can
-- see exactly how fetches are batched.
module Main (main) where

import Fetch

import Control.Monad (when)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..))
import Data.IORef
import Data.List (intercalate)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import qualified Data.List.NonEmpty as NE
import Database.SQLite.Simple
import GHC.Generics (Generic)

-- ══════════════════════════════════════════════
-- Domain types
-- ══════════════════════════════════════════════

data User = User
  { userId   :: !Int
  , userName :: !Text
  } deriving (Show, Eq, Generic)

instance FromRow User where
  fromRow = User <$> field <*> field

data Post = Post
  { postId       :: !Int
  , postAuthorId :: !Int
  , postTitle    :: !Text
  , postBody     :: !Text
  } deriving (Show, Eq, Generic)

instance FromRow Post where
  fromRow = Post <$> field <*> field <*> field <*> field

data Comment = Comment
  { commentId       :: !Int
  , commentPostId   :: !Int
  , commentAuthorId :: !Int
  , commentBody     :: !Text
  } deriving (Show, Eq, Generic)

instance FromRow Comment where
  fromRow = Comment <$> field <*> field <*> field <*> field

-- ══════════════════════════════════════════════
-- FetchKey types
-- ══════════════════════════════════════════════

newtype UserById = UserById Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserById where
  type Result UserById = User

newtype PostById = PostById Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey PostById where
  type Result PostById = Post

newtype PostsByAuthor = PostsByAuthor Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey PostsByAuthor where
  type Result PostsByAuthor = [Post]

newtype CommentsByPost = CommentsByPost Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey CommentsByPost where
  type Result CommentsByPost = [Comment]

-- | Count of comments on a post (facet: lightweight aggregate).
newtype CommentCountByPost = CommentCountByPost Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey CommentCountByPost where
  type Result CommentCountByPost = Int

-- | Most recent comment on a post (facet: preview snippet).
newtype LatestCommentByPost = LatestCommentByPost Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey LatestCommentByPost where
  type Result LatestCommentByPost = Maybe Comment

-- ══════════════════════════════════════════════
-- Application monad
-- ══════════════════════════════════════════════

-- | A thin Reader-over-IO carrying a SQLite connection.
-- No MTL — just manual instances, same style as in the test suite.
newtype AppM a = AppM { unAppM :: Connection -> IO a }

instance Functor AppM where
  fmap f (AppM g) = AppM $ \c -> fmap f (g c)

instance Applicative AppM where
  pure a = AppM $ \_ -> pure a
  AppM ff <*> AppM fx = AppM $ \c -> ff c <*> fx c

instance Monad AppM where
  AppM ma >>= f = AppM $ \c -> do
    a <- ma c
    unAppM (f a) c

askConn :: AppM Connection
askConn = AppM pure

liftIO :: IO a -> AppM a
liftIO io = AppM $ \_ -> io

runAppM :: Connection -> AppM a -> IO a
runAppM conn (AppM f) = f conn

-- ══════════════════════════════════════════════
-- DataSource instances (batched SQL)
-- ══════════════════════════════════════════════

-- | Helper: build a SQL IN clause with the right number of placeholders.
inClause :: Int -> Query
inClause n = Query $ "(" <> T.intercalate "," (replicate n "?") <> ")"

instance DataSource AppM UserById where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | UserById i <- keys]
        n    = length ids
        sql  = "SELECT id, name FROM users WHERE id IN " <> inClause n
    liftIO $ putStrLn $ "  [SQL] SELECT id, name FROM users WHERE id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIO $ query conn sql ids
    pure $ HM.fromList [(UserById (userId u), u) | u <- rows]

instance DataSource AppM PostById where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | PostById i <- keys]
        n    = length ids
        sql  = "SELECT id, author_id, title, body FROM posts WHERE id IN " <> inClause n
    liftIO $ putStrLn $ "  [SQL] SELECT ... FROM posts WHERE id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIO $ query conn sql ids
    pure $ HM.fromList [(PostById (postId p), p) | p <- rows]

instance DataSource AppM PostsByAuthor where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | PostsByAuthor i <- keys]
        n    = length ids
        sql  = "SELECT id, author_id, title, body FROM posts WHERE author_id IN " <> inClause n
    liftIO $ putStrLn $ "  [SQL] SELECT ... FROM posts WHERE author_id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIO $ query conn sql ids
    -- Group posts by author_id
    let grouped = HM.fromListWith (++) [(PostsByAuthor (postAuthorId p), [p]) | p <- rows]
    -- Ensure every requested key has an entry (empty list if no posts)
    pure $ HM.union grouped (HM.fromList [(k, []) | k <- keys])

instance DataSource AppM CommentsByPost where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | CommentsByPost i <- keys]
        n    = length ids
        sql  = "SELECT id, post_id, author_id, body FROM comments WHERE post_id IN " <> inClause n
    liftIO $ putStrLn $ "  [SQL] SELECT ... FROM comments WHERE post_id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIO $ query conn sql ids
    let grouped = HM.fromListWith (++) [(CommentsByPost (commentPostId c), [c]) | c <- rows]
    pure $ HM.union grouped (HM.fromList [(k, []) | k <- keys])

instance DataSource AppM CommentCountByPost where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | CommentCountByPost i <- keys]
        n    = length ids
        sql  = "SELECT post_id, COUNT(*) FROM comments WHERE post_id IN "
            <> inClause n <> " GROUP BY post_id"
    liftIO $ putStrLn $ "  [SQL] SELECT post_id, COUNT(*) FROM comments WHERE post_id IN ("
      <> intercalate ", " (map show ids) <> ") GROUP BY post_id"
    rows <- liftIO $ query conn sql ids :: AppM [(Int, Int)]
    let counts = HM.fromList [(CommentCountByPost pid, cnt) | (pid, cnt) <- rows]
    -- Posts with no comments won't appear in GROUP BY — default to 0
    pure $ HM.union counts (HM.fromList [(k, 0) | k <- keys])

instance DataSource AppM LatestCommentByPost where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | LatestCommentByPost i <- keys]
        n    = length ids
        -- Grab all comments for the requested posts, we'll pick the latest per post in Haskell.
        -- (SQLite doesn't have a clean single-query "latest per group" for batched IN.)
        sql  = "SELECT id, post_id, author_id, body FROM comments WHERE post_id IN "
            <> inClause n <> " ORDER BY id DESC"
    liftIO $ putStrLn $ "  [SQL] SELECT ... FROM comments WHERE post_id IN ("
      <> intercalate ", " (map show ids) <> ") ORDER BY id DESC"
    rows <- liftIO $ query conn sql ids
    -- Take the first (latest by id) comment per post
    let byPost = HM.fromListWith (\_ older -> older)
                   [(LatestCommentByPost (commentPostId c), c) | c <- rows]
        withMaybe = HM.map Just byPost
    pure $ HM.union withMaybe (HM.fromList [(k, Nothing) | k <- keys])

-- | A key type that chunks its SQL IN clauses to avoid oversized queries.
-- Same result as UserById, but the DataSource splits large batches.
newtype UserByIdChunked = UserByIdChunked Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserByIdChunked where
  type Result UserByIdChunked = User

instance DataSource AppM UserByIdChunked where
  batchFetch keysNE = do
    conn <- askConn
    let keys = NE.toList keysNE
        ids  = [i | UserByIdChunked i <- keys]
        -- Chunk into groups of 50 to keep IN clauses manageable
        chunks = chunksOf 50 ids
    liftIO $ putStrLn $ "  [SQL] Chunking " <> show (length ids)
      <> " keys into " <> show (length chunks) <> " chunk(s) of <= 50"
    results <- mapM (\chunk -> do
      let n = length chunk
          sql = "SELECT id, name FROM users WHERE id IN " <> inClause n
      liftIO $ putStrLn $ "  [SQL]   chunk: SELECT id, name FROM users WHERE id IN (<"
        <> show n <> " ids>)"
      liftIO $ query conn sql chunk
      ) chunks
    pure $ HM.fromList [(UserByIdChunked (userId u), u) | u <- concat results]

-- ══════════════════════════════════════════════
-- Restricted DB monad (no MonadIO)
-- ══════════════════════════════════════════════

-- | A restricted monad mimicking Mercury's DB type.
--
-- Structurally identical to AppM (ReaderT Connection IO), but
-- deliberately has NO MonadIO instance. Arbitrary IO inside
-- database transactions is forbidden at the type level.
--
-- sofetch works perfectly with this pattern: the unsafe nats
-- needed by the engine are private to the module that defines
-- the runner. Application code never sees them.
newtype DB a = DB { unDB :: Connection -> IO a }

instance Functor DB where
  fmap f (DB g) = DB $ \c -> fmap f (g c)

instance Applicative DB where
  pure a = DB $ \_ -> pure a
  DB ff <*> DB fx = DB $ \c -> ff c <*> fx c

instance Monad DB where
  DB ma >>= f = DB $ \c -> do
    a <- ma c
    unDB (f a) c

instance MonadThrow DB where
  throwM e = DB $ \_ -> throwM e

instance MonadCatch DB where
  catch (DB f) handler = DB $ \c ->
    catch (f c) (\e -> unDB (handler e) c)

-- Intentionally NO MonadIO instance. Any code that tries
-- @liftIO@ in DB gets a compile error: "No instance for MonadIO DB".
-- In production (Mercury), a TypeError instance provides a custom
-- message. Here the missing instance suffices.

-- ── Escape hatches (private to your DB module) ──

-- | Lift IO into DB. Not exported from your DB module in production.
-- sofetch's engine needs this internally for cache and IVar operations.
unsafeLiftIODB :: IO a -> DB a
unsafeLiftIODB io = DB $ \_ -> io

-- | Lower DB to IO, given a connection. Also not exported.
unsafeRunDB :: Connection -> DB a -> IO a
unsafeRunDB conn (DB f) = f conn

-- ── DB-internal helpers (like askConn / liftIO for AppM) ──

askConnDB :: DB Connection
askConnDB = DB pure

liftIODB :: IO a -> DB a
liftIODB = unsafeLiftIODB

-- ── DataSource instances for DB ─────────────

instance DataSource DB UserById where
  batchFetch keysNE = do
    conn <- askConnDB
    let keys = NE.toList keysNE
        ids  = [i | UserById i <- keys]
        n    = length ids
        sql  = "SELECT id, name FROM users WHERE id IN " <> inClause n
    liftIODB $ putStrLn $ "  [SQL] SELECT id, name FROM users WHERE id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIODB $ query conn sql ids
    pure $ HM.fromList [(UserById (userId u), u) | u <- rows]

instance DataSource DB PostsByAuthor where
  batchFetch keysNE = do
    conn <- askConnDB
    let keys = NE.toList keysNE
        ids  = [i | PostsByAuthor i <- keys]
        n    = length ids
        sql  = "SELECT id, author_id, title, body FROM posts WHERE author_id IN " <> inClause n
    liftIODB $ putStrLn $ "  [SQL] SELECT ... FROM posts WHERE author_id IN ("
      <> intercalate ", " (map show ids) <> ")"
    rows <- liftIODB $ query conn sql ids
    let grouped = HM.fromListWith (++) [(PostsByAuthor (postAuthorId p), [p]) | p <- rows]
    pure $ HM.union grouped (HM.fromList [(k, []) | k <- keys])

-- ── The SAFE public runner ──────────────────

-- | Run a Fetch computation inside the restricted DB monad.
--
-- This is the ONLY function your module exports. The unsafe nats
-- (unsafeRunDB, unsafeLiftIODB) stay private. Application code
-- writes against @MonadFetch DB n@ and never touches IO.
--
-- This is the pattern from the sofetch docs:
--
-- @
-- fetchInTransaction :: Fetch Transaction a -> Transaction a
-- fetchInTransaction = runFetchIO unsafeRunTransaction unsafeLiftIO
-- @
fetchInDB :: Fetch DB a -> DB a
fetchInDB action = DB $ \conn ->
  unsafeRunDB conn $ runFetch (fetchConfig (unsafeRunDB conn) unsafeLiftIODB) action

-- ══════════════════════════════════════════════
-- Polymorphic data-access functions
-- ══════════════════════════════════════════════

-- ──────────────────────────────────────────────
-- Faceted search result card
-- ──────────────────────────────────────────────

-- | A search result card assembled from multiple independent facets.
-- Each facet is a different data source; sofetch batches them all.
data SearchCard = SearchCard
  { cardTitle        :: !Text
  , cardAuthor       :: !Text
  , cardCommentCount :: !Int
  , cardPreview      :: !(Maybe Text)  -- latest comment body, if any
  } deriving (Show)

-- | Build a search card for a single post. Four independent facets:
--   1. Post title/body    (PostById)
--   2. Author name        (UserById — depends on post's author_id)
--   3. Comment count       (CommentCountByPost)
--   4. Latest comment      (LatestCommentByPost)
--
-- Facets 1, 3, 4 are independent of each other and batch in one round.
-- Facet 2 depends on knowing the author_id from facet 1, adding a second round.
-- When called via 'traverse' across many post IDs, ALL posts' facets
-- merge — so N posts still need at most 2 rounds, not 4*N queries.
buildSearchCard
  :: ( MonadFetch m n
     , DataSource m PostById
     , DataSource m UserById
     , DataSource m CommentCountByPost
     , DataSource m LatestCommentByPost
     )
  => Int -> n SearchCard
buildSearchCard pid = do
  -- Round 1: three independent facets batch together
  (post, count, latest) <-
    (,,) <$> fetch (PostById pid)
         <*> fetch (CommentCountByPost pid)
         <*> fetch (LatestCommentByPost pid)
  -- Round 2: author lookup depends on the post's author_id
  author <- fetch (UserById (postAuthorId post))
  pure SearchCard
    { cardTitle        = postTitle post
    , cardAuthor       = userName author
    , cardCommentCount = count
    , cardPreview      = fmap commentBody latest
    }

-- | Build search cards for a list of post IDs.
-- All cards' facets batch across the entire list.
buildSearchResults
  :: ( MonadFetch m n
     , DataSource m PostById
     , DataSource m UserById
     , DataSource m CommentCountByPost
     , DataSource m LatestCommentByPost
     )
  => [Int] -> n [SearchCard]
buildSearchResults = traverse buildSearchCard

-- ──────────────────────────────────────────────

-- | Fetch a user and their posts. Polymorphic over the fetch monad,
-- so it works with both Fetch (production) and MockFetch (tests).
getUserFeed
  :: ( MonadFetch m n
     , DataSource m UserById
     , DataSource m PostsByAuthor
     )
  => Int -> n (User, [Post])
getUserFeed uid = (,) <$> fetch (UserById uid) <*> fetch (PostsByAuthor uid)

-- ──────────────────────────────────────────────
-- Deep N+1: functions that compose across levels
-- ──────────────────────────────────────────────

-- Each function below is written independently — it doesn't know
-- whether it will be called once or a thousand times. In a normal
-- monadic framework, calling these in a loop creates an N+1 cascade
-- (one query per iteration). With sofetch, traverse uses Applicative
-- so all iterations at the same depth batch into a single round.

-- | Level 3 (leaf): render one comment -> (body, authorName).
-- Fetches the comment's author.
renderComment
  :: (MonadFetch m n, DataSource m UserById)
  => Comment -> n (Text, Text)
renderComment c = do
  author <- fetch (UserById (commentAuthorId c))
  pure (commentBody c, userName author)

-- | Level 2: render a post with all its comments.
-- Fetches comments, then traverses into renderComment.
renderPostWithComments
  :: (MonadFetch m n, DataSource m UserById, DataSource m CommentsByPost)
  => Post -> n (Text, [(Text, Text)])
renderPostWithComments p = do
  comments <- fetch (CommentsByPost (postId p))
  rendered <- traverse renderComment comments
  pure (postTitle p, rendered)

-- | Level 1: render an author's full profile.
-- Fetches user + posts, then traverses into renderPostWithComments.
renderAuthorProfile
  :: ( MonadFetch m n
     , DataSource m UserById
     , DataSource m PostsByAuthor
     , DataSource m CommentsByPost
     )
  => Int -> n (Text, [(Text, [(Text, Text)])])
renderAuthorProfile uid = do
  user  <- fetch (UserById uid)
  posts <- fetch (PostsByAuthor uid)
  renderedPosts <- traverse renderPostWithComments posts
  pure (userName user, renderedPosts)

-- | Top level: render the blog page for multiple authors.
-- Calls renderAuthorProfile for each — traverse batches them.
renderBlogPage
  :: ( MonadFetch m n
     , DataSource m UserById
     , DataSource m PostsByAuthor
     , DataSource m CommentsByPost
     )
  => [Int] -> n [(Text, [(Text, [(Text, Text)])])]
renderBlogPage = traverse renderAuthorProfile

-- ══════════════════════════════════════════════
-- Helpers
-- ══════════════════════════════════════════════

-- | Split a list into chunks of at most n elements.
chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf n xs = let (chunk, rest) = splitAt n xs in chunk : chunksOf n rest

-- ══════════════════════════════════════════════
-- Database setup
-- ══════════════════════════════════════════════

setupDatabase :: Connection -> IO ()
setupDatabase conn = do
  execute_ conn "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
  execute_ conn "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL)"
  execute_ conn "CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, author_id INTEGER NOT NULL, body TEXT NOT NULL)"

  -- 5 named users + 200 generated users for the chunking demo
  let namedUsers :: [(Int, Text)]
      namedUsers =
        [ (1, "Alice"),   (2, "Bob"),    (3, "Carol")
        , (4, "Dave"),    (5, "Eve")
        ]
      generatedUsers = [(i, "User_" <> T.pack (show i)) | i <- [6..205]]
  mapM_ (\(i, n) -> execute conn "INSERT INTO users VALUES (?, ?)" (i :: Int, n :: Text))
    (namedUsers ++ generatedUsers)

  -- 8 posts across 3 authors
  let posts :: [(Int, Int, Text, Text)]
      posts =
        [ (1, 1, "Getting Started with Haskell", "Haskell is a purely functional language...")
        , (2, 1, "Type Families Explained",      "Type families let you compute types...")
        , (3, 2, "Why I Love Rust",              "Memory safety without GC...")
        , (4, 2, "Async Rust Patterns",          "Tokio makes async Rust ergonomic...")
        , (5, 3, "Intro to Category Theory",     "A category consists of objects and morphisms...")
        , (6, 1, "Monad Transformers in Practice","Stacking monads with transformers...")
        , (7, 3, "Functors Are Everywhere",      "From Maybe to IO, functors are...")
        , (8, 4, "SQLite Tips and Tricks",        "SQLite is surprisingly powerful...")
        ]
  mapM_ (\(i, a, t, b) -> execute conn "INSERT INTO posts VALUES (?, ?, ?, ?)" (i :: Int, a :: Int, t :: Text, b :: Text)) posts

  -- 12 comments, deliberately with overlapping authors across posts
  let comments :: [(Int, Int, Int, Text)]
      comments =
        [ (1,  1, 2, "Great intro!")
        , (2,  1, 3, "I learned a lot from this.")
        , (3,  2, 2, "Type families are tricky but powerful.")
        , (4,  2, 5, "Can you cover associated types next?")
        , (5,  3, 1, "Interesting perspective on Rust.")
        , (6,  3, 4, "How does it compare to Haskell?")
        , (7,  4, 3, "Tokio is amazing.")
        , (8,  5, 2, "Category theory is fascinating!")
        , (9,  5, 4, "The diagrams really help.")
        , (10, 6, 3, "Monad transformers clicked for me here.")
        , (11, 7, 1, "Functors everywhere indeed!")
        , (12, 8, 5, "I use SQLite for everything.")
        ]
  mapM_ (\(i, p, a, b) -> execute conn "INSERT INTO comments VALUES (?, ?, ?, ?)" (i :: Int, p :: Int, a :: Int, b :: Text)) comments

-- ══════════════════════════════════════════════
-- Instrumented runner
-- ══════════════════════════════════════════════

-- | Run a Fetch computation with detailed round-by-round logging.
-- Shows round boundaries, key counts, sources dispatched, and cache hits.
runFetchIO :: Connection -> Fetch AppM a -> IO a
runFetchIO conn action = do
  cRef <- newCacheRef
  totalRoundsRef <- newIORef (0 :: Int)
  totalKeysRef   <- newIORef (0 :: Int)
  totalHitsRef   <- newIORef (0 :: Int)

  let e = FetchEnv
        { fetchCache = cRef
        , fetchLower = runAppM conn
        , fetchLift  = Main.liftIO
        }

      withRound n batches exec = do
        let pending  = batchSize batches
            sources  = batchSourceCount batches
        Main.liftIO $ putStrLn $ "  \x250c\x2500 Round " <> show n
          <> ": " <> show pending <> " key(s) across "
          <> show sources <> " source(s)"

        stats <- exec

        let dispatched = roundKeys stats - roundCacheHits stats
        Main.liftIO $ do
          when (roundCacheHits stats > 0) $
            putStrLn $ "  \x2502  Cache: " <> show (roundCacheHits stats) <> " hit(s)"
          putStrLn $ "  \x2502  Dispatched: " <> show dispatched <> " key(s) to data sources"
          putStrLn $ "  \x2514\x2500 Round " <> show n <> " complete"
          modifyIORef' totalRoundsRef (+ 1)
          modifyIORef' totalKeysRef (+ roundKeys stats)
          modifyIORef' totalHitsRef (+ roundCacheHits stats)

  a <- runAppM conn $ runLoopWith e withRound action

  rounds <- readIORef totalRoundsRef
  keys   <- readIORef totalKeysRef
  hits   <- readIORef totalHitsRef
  putStrLn $ "  \x2500\x2500 Summary: " <> show rounds <> " round(s), "
    <> show keys <> " key(s), "
    <> show hits <> " cache hit(s)"
  pure a

-- | Like 'runFetchIO' but with an externally-provided cache.
-- Useful for sharing cache across multiple computations.
runFetchIOWithCache :: Connection -> CacheRef -> Fetch AppM a -> IO a
runFetchIOWithCache conn cRef action = do
  let e = FetchEnv
        { fetchCache = cRef
        , fetchLower = runAppM conn
        , fetchLift  = Main.liftIO
        }

      withRound n batches exec = do
        let pending  = batchSize batches
            sources  = batchSourceCount batches
        Main.liftIO $ putStrLn $ "  \x250c\x2500 Round " <> show n
          <> ": " <> show pending <> " key(s) across "
          <> show sources <> " source(s)"

        stats <- exec

        let dispatched = roundKeys stats - roundCacheHits stats
        Main.liftIO $ do
          when (roundCacheHits stats > 0) $
            putStrLn $ "  \x2502  Cache: " <> show (roundCacheHits stats) <> " hit(s), skipped"
          when (dispatched > 0) $
            putStrLn $ "  \x2502  Dispatched: " <> show dispatched <> " key(s) to data sources"
          putStrLn $ "  \x2514\x2500 Round " <> show n <> " complete"

  runAppM conn $ runLoopWith e withRound action

-- ══════════════════════════════════════════════
-- Scenarios
-- ══════════════════════════════════════════════

header :: String -> String -> IO ()
header num desc = do
  putStrLn ""
  putStrLn $ "\x2501\x2501\x2501 Scenario " <> num <> ": " <> desc <> " \x2501\x2501\x2501"

main :: IO ()
main = do
  conn <- open ":memory:"
  setupDatabase conn
  putStrLn "Database seeded with 205 users, 8 posts, 12 comments."

  -- ── Scenario 1: Single fetch ──────────────────
  header "1" "Single fetch"
  putStrLn "Fetching a single user by ID."
  user <- runFetchIO conn $ fetch (UserById 1)
  putStrLn $ "  => " <> show user

  -- ── Scenario 2: Applicative batching ──────────
  header "2" "Applicative batching (two sources, one round)"
  putStrLn "Fetching a user AND their posts in one round (two different sources)."
  (u, ps) <- runFetchIO conn $
    (,) <$> fetch (UserById 1) <*> fetch (PostsByAuthor 1)
  putStrLn $ "  => User: " <> show (userName u)
  putStrLn $ "  => Posts: " <> show (map postTitle ps)

  -- ── Scenario 3: Monadic dependency ────────────
  header "3" "Monadic dependency (2 rounds)"
  putStrLn "Fetching a post, THEN its author (data dependency forces 2 rounds)."
  (post, author) <- runFetchIO conn $ do
    p <- fetch (PostById 3)                   -- round 1
    a <- fetch (UserById (postAuthorId p))    -- round 2 (depends on post)
    pure (p, a)
  putStrLn $ "  => Post: \"" <> T.unpack (postTitle post)
    <> "\" by " <> T.unpack (userName author)

  -- ── Scenario 4: N+1 avoidance ────────────────
  header "4" "N+1 avoidance"
  putStrLn "Fetching Alice's posts, then ALL comments for those posts in ONE batch."
  putStrLn "(Without sofetch, this would be N separate queries.)"
  allComments <- runFetchIO conn $ do
    posts <- fetch (PostsByAuthor 1)                         -- round 1
    fetchAll (map (CommentsByPost . postId) posts)           -- round 2: ONE batch
  putStrLn $ "  => Comment counts per post: " <> show (map length allComments)

  -- ── Scenario 5: Deduplication ────────────────
  header "5" "Deduplication"
  putStrLn "Fetching comments on posts 1 and 2. Bob (id=2) commented on both."
  putStrLn "His user record should only be fetched ONCE."
  authors <- runFetchIO conn $ do
    -- Round 1: both comment fetches batch into ONE SQL query
    (c1, c2) <- (,) <$> fetch (CommentsByPost 1) <*> fetch (CommentsByPost 2)
    -- Round 2: author IDs are deduplicated — Bob only fetched once
    let authorIds = map commentAuthorId (c1 ++ c2)
    liftSource $ Main.liftIO $ putStrLn $ "  \x2502  Raw author IDs: " <> show authorIds
      <> " (4 refs, but Bob=2 appears twice)"
    fetchAll (map UserById authorIds)
  putStrLn $ "  => Authors: " <> show (map userName authors)

  -- ── Scenario 6: Combinators ──────────────────
  header "6" "Combinators (fetchThrough, fetchMap)"
  putStrLn "Using fetchThrough to enrich comments with their authors:"
  enriched <- runFetchIO conn $ do
    comments <- fetch (CommentsByPost 1)
    fetchThrough (UserById . commentAuthorId) comments
  mapM_ (\(c, author') -> putStrLn $ "  => " <> T.unpack (commentBody c)
    <> " -- " <> T.unpack (userName author')) enriched

  putStrLn ""
  putStrLn "Using fetchMap to get (title, authorName) from post IDs:"
  results <- runFetchIO conn $
    fetchMap PostById (\_ p -> (postTitle p, postAuthorId p)) [1, 3, 5]
  mapM_ (\(title, aid) -> putStrLn $ "  => \"" <> T.unpack title
    <> "\" (author_id=" <> show aid <> ")") results

  -- ── Scenario 7: Cache hits in action ──────────
  header "7" "Cache hits in action"
  putStrLn "Sharing a cache across TWO separate computations."
  putStrLn "The second computation benefits from the first's cache."
  cRef <- newCacheRef
  -- First computation: fills the cache
  putStrLn ""
  putStrLn "  First computation (cold cache) -- requesting UserById 1, 2:"
  _ <- runFetchIOWithCache conn cRef $
    (,) <$> fetch (UserById 1) <*> fetch (UserById 2)
  -- Second computation: same cache
  putStrLn ""
  putStrLn "  Second computation (warm cache) -- requesting UserById 1, 2, 3:"
  putStrLn "  Users 1 and 2 resolve instantly from cache. Only user 3 hits SQL."
  _ <- runFetchIOWithCache conn cRef $
    (,,) <$> fetch (UserById 1) <*> fetch (UserById 2) <*> fetch (UserById 3)
  pure ()

  -- ── Scenario 8: MockFetch ────────────────────
  header "8" "MockFetch (same code, no database)"
  putStrLn "Running getUserFeed against REAL SQLite:"
  realResult <- runFetchIO conn $ getUserFeed 1
  putStrLn $ "  => " <> show (userName (fst realResult))
    <> ", " <> show (length (snd realResult)) <> " posts"

  putStrLn ""
  putStrLn "Running the EXACT SAME function against MockFetch (canned data):"
  let mockUser = User 1 "Mock Alice"
      mockPosts = [Post 99 1 "Mock Post" "This is fake data"]
      mocks = mockData @UserById [(UserById 1, mockUser)]
           <> mockData @PostsByAuthor [(PostsByAuthor 1, mockPosts)]
  mockResult <- runMockFetch @AppM mocks (getUserFeed 1)
  putStrLn $ "  => " <> show (userName (fst mockResult))
    <> ", " <> show (length (snd mockResult)) <> " posts"

  -- ── Scenario 9: Deep N+1 across function boundaries ──
  header "9" "Deep N+1 across function boundaries"
  putStrLn "renderBlogPage calls renderAuthorProfile for 3 authors,"
  putStrLn "which calls renderPostWithComments for each post,"
  putStrLn "which calls renderComment for each comment."
  putStrLn ""
  putStrLn "In a for-loop world this would be dozens of queries."
  putStrLn "With sofetch, each depth level batches into ONE round:"
  putStrLn ""
  profiles <- runFetchIO conn $ renderBlogPage [1, 2, 3]
  putStrLn ""
  putStrLn "  Results:"
  mapM_ (\(authorName', postSummaries) -> do
    putStrLn $ "  " <> T.unpack authorName' <> ":"
    mapM_ (\(title, commentSummaries) -> do
      putStrLn $ "    \"" <> T.unpack title <> "\" ("
        <> show (length commentSummaries) <> " comments)"
      ) postSummaries
    ) profiles

  -- ── Scenario 10: Chunked batching for large key sets ──
  header "10" "Chunked batching for large key sets"
  putStrLn "Fetching 200 users at once. The UserByIdChunked data source"
  putStrLn "splits the IN clause into chunks of 50 to avoid oversized SQL."
  putStrLn ""
  users200 <- runFetchIO conn $
    fetchAll (map UserByIdChunked [1..200])
  putStrLn $ "  => Fetched " <> show (length users200) <> " users"
  putStrLn $ "  => First: " <> show (take 1 users200)
  putStrLn $ "  => Last:  " <> show (take 1 (reverse users200))

  -- ── Scenario 11: Faceted queries ────────────────
  header "11" "Faceted queries (search result cards)"
  putStrLn "Building search result cards for 5 posts. Each card needs 4 facets:"
  putStrLn "  - post title/body  (PostById)"
  putStrLn "  - author name      (UserById, depends on post)"
  putStrLn "  - comment count    (CommentCountByPost)"
  putStrLn "  - latest comment   (LatestCommentByPost)"
  putStrLn ""
  putStrLn "For 5 posts, that's 20 potential queries. With sofetch:"
  putStrLn "  Round 1: all PostById + CommentCountByPost + LatestCommentByPost"
  putStrLn "  Round 2: all UserById (depends on author_id from round 1)"
  putStrLn ""
  cards <- runFetchIO conn $ buildSearchResults [1, 2, 3, 5, 8]
  putStrLn ""
  putStrLn "  Results:"
  mapM_ (\c -> putStrLn $ "  \"" <> T.unpack (cardTitle c) <> "\""
    <> " by " <> T.unpack (cardAuthor c)
    <> " (" <> show (cardCommentCount c) <> " comments)"
    <> maybe "" (\p -> " -- latest: \"" <> T.unpack p <> "\"") (cardPreview c)
    ) cards

  -- ── Scenario 12: Restricted DB monad (no MonadIO) ──
  header "12" "Restricted DB monad (no MonadIO)"
  putStrLn "The DB monad has NO MonadIO instance — arbitrary IO inside"
  putStrLn "database transactions is a compile-time error."
  putStrLn ""
  putStrLn "sofetch works via fetchInDB, which hides the unsafe nats."
  putStrLn "The same polymorphic getUserFeed function works in DB:"
  putStrLn ""

  -- getUserFeed is polymorphic: (MonadFetch m n, DataSource m UserById, ...)
  -- It works with both AppM (via runFetchIO) and DB (via fetchInDB).
  let runDB :: DB a -> IO a
      runDB act = unsafeRunDB conn act

  dbResult <- runDB $ fetchInDB $ getUserFeed 1
  putStrLn $ "  => User: " <> T.unpack (userName (fst dbResult))
  putStrLn $ "  => Posts: " <> show (length (snd dbResult))

  -- Prove the TypeError works by showing the error message.
  -- (Uncomment the line below to see the compile-time error:)
  -- _ <- runDB $ Control.Monad.IO.Class.liftIO (putStrLn "this won't compile")

  close conn
  putStrLn ""
  putStrLn "Done!"
