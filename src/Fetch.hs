{-# LANGUAGE TypeFamilies #-}

-- | Automatic batching and deduplication of concurrent data fetches.
--
-- = Background
--
-- Any service that assembles responses from multiple data sources runs into
-- the same problem: you write sequential-looking code, but the access
-- pattern it produces is terrible. Fetching a user, then their posts, then
-- the author of each post, produces a cascade of round trips: the classic
-- N+1 query problem, generalised across arbitrary backends.
--
-- Facebook's [Haxl](https://github.com/facebook/Haxl) library (Marlow et al.,
-- /\"There is no Fork: an Abstraction for Efficient, Concurrent, and Concise
-- Data Access\"/, ICFP 2014) solved this by exploiting @Applicative@ to
-- detect independent data fetches and batch them into a single round.
-- Code that /looks/ sequential gets automatically optimised into concurrent,
-- batched requests, with request deduplication and caching for free.
--
-- This library keeps Haxl's core idea while simplifying the machinery
-- required to use it:
--
--   * __No GADTs in user code.__ Haxl encodes the request\/response type
--     pairing with a GADT indexed by the result type. Here, an associated
--     type family ('Result') on an ordinary typeclass ('FetchKey') does the
--     same job. Your key types derive 'Eq', 'Hashable', and 'Show' with
--     stock @deriving@.
--
--   * __Data sources run in your monad.__ 'DataSource' is parameterised by
--     a monad @m@, not a concrete environment type. If your data source
--     needs a connection pool, @m@ should be a monad with access to one
--     (e.g. via 'MonadReader'). If the instance doesn't exist, code that
--     tries to @fetch@ a key won't compile. No runtime "missing config"
--     errors.
--
--   * __Monad transformer, not a concrete monad.__ Haxl's @GenHaxl u w@ is
--     a fixed monad. 'Fetch' is a transformer over your source monad @m@.
--     Two natural transformations (@m x -> IO x@ and @IO x -> m x@) provided
--     at the run site bridge the source monad with IO for internal
--     concurrency and caching.
--
--   * __Swappable implementations via 'MonadFetch'.__ Production code,
--     traced instrumentation, and pure mock testing all share the same
--     interface. Application functions are polymorphic over the implementation.
--
-- = How batching works
--
-- 'Fetch' has an 'Applicative' instance that /merges/ the pending fetches
-- from both sides of @\<*\>@ into one round, and a 'Monad' instance where
-- @>>=@ is a round boundary (the right side can't run until the left side's
-- results are available).
--
-- With @ApplicativeDo@ enabled, GHC desugars @do@-blocks into 'Applicative'
-- combinators wherever the data dependencies allow it. Two @fetch@ calls
-- whose results are independent of each other will be combined into a
-- single batch, even though they appear on separate lines:
--
-- @
-- {-# LANGUAGE ApplicativeDo #-}
--
-- getUserFeed :: (MonadFetch m n, DataSource m UserId, DataSource m PostsByAuthor)
--             => UserId -> n Feed
-- getUserFeed uid = do
--   user  <- fetch uid                  -- ─┐
--   posts <- fetch (PostsByAuthor uid)  -- ─┤ same round
--   pure (Feed user posts)
-- @
--
-- If a later fetch /depends/ on an earlier result, @>>=@ forces a round
-- boundary and the fetches run in sequence:
--
-- @
-- getUserThenManager :: (MonadFetch m n, DataSource m UserId)
--                    => UserId -> n (User, User)
-- getUserThenManager uid = do
--   user    <- fetch uid                   -- round 1
--   manager <- fetch (managerId user)      -- round 2 (depends on user)
--   pure (user, manager)
-- @
--
-- Within each round, keys destined for the same data source are grouped and
-- passed to 'batchFetch' together. Keys for /different/ sources run
-- concurrently by default (see 'FetchStrategy'). Duplicate keys are
-- deduplicated across the entire computation.
--
-- = Tutorial
--
-- == Step 1: Define your key types
--
-- Each type of data you want to fetch gets its own key type with a
-- 'FetchKey' instance that declares the result type:
--
-- @
-- newtype UserId = UserId Int
--   deriving (Eq, Hashable, Show)
--
-- instance FetchKey UserId where
--   type Result UserId = User
--
-- newtype PostsByAuthor = PostsByAuthor Int
--   deriving (Eq, Hashable, Show)
--
-- instance FetchKey PostsByAuthor where
--   type Result PostsByAuthor = [Post]
-- @
--
-- Each key type maps to exactly one result type. Separate types per query
-- give you stock @deriving@, first-class 'Data.HashMap.Strict.HashMap' keys, and
-- precise constraints: a function's type signature advertises exactly
-- which data sources it touches.
--
-- == Step 2: Define your data sources
--
-- A 'DataSource' instance tells the engine how to batch-fetch a list of
-- keys. The monad @m@ provides any resources the source needs:
--
-- @
-- data AppEnv = AppEnv
--   { appPool  :: ConnectionPool
--   , appRedis :: RedisConn
--   }
--
-- -- AppM is a ReaderT-like monad carrying the environment.
-- newtype AppM a = AppM (ReaderT AppEnv IO a)
--
-- instance DataSource AppM UserId where
--   batchFetch ids = do
--     pool <- asks appPool
--     liftIO $ withResource pool $ \\conn -> do
--       rows <- query conn \"SELECT id, name FROM users WHERE id = ANY(?)\" (Only ids)
--       pure (HM.fromList [(UserId i, User i n) | (i, n) <- rows])
--
-- instance DataSource AppM PostsByAuthor where
--   batchFetch ks = do
--     pool <- asks appPool
--     liftIO $ withResource pool $ \\conn -> do
--       let authorIds = [aid | PostsByAuthor aid <- ks]
--       rows <- query conn \"SELECT author_id, id, body FROM posts WHERE author_id = ANY(?)\" (Only authorIds)
--       let grouped = HM.fromListWith (<>) [(PostsByAuthor aid, [Post pid body]) | (aid, pid, body) <- rows]
--       pure grouped
-- @
--
-- The return type is @'Data.HashMap.Strict.HashMap' k ('Result' k)@: you must return
-- a result for every key you were given. The engine handles concurrency,
-- caching, and error wrapping around your function.
--
-- If the @DataSource AppM SomeKey@ instance doesn't exist, any code that
-- tries to @fetch@ a @SomeKey@ will fail to compile. There are no runtime
-- \"missing config\" errors.
--
-- == Step 3: Write data-access code
--
-- Program against the 'MonadFetch' constraint. Don't commit to a specific
-- implementation. This is what makes the same code runnable in production
-- and in tests:
--
-- @
-- {-# LANGUAGE ApplicativeDo #-}
--
-- getUserFeed :: (MonadFetch m n, DataSource m UserId, DataSource m PostsByAuthor)
--             => UserId -> n Feed
-- getUserFeed uid = do
--   user  <- fetch uid
--   posts <- fetch (PostsByAuthor uid)
--   pure (Feed user posts)
-- @
--
-- For fetching across collections, use the provided combinators to preserve
-- the container shape without manual destructure\/reconstruct cycles:
--
-- @
-- enrichComments :: (MonadFetch m n, DataSource m CommentAuthor)
--                => [Comment] -> n [(Comment, User)]
-- enrichComments = fetchThrough commentAuthor
-- @
--
-- == Step 4: Run it
--
-- In production, use 'runFetch' with two natural transformations: one
-- to lower @m@ to @IO@, and one to lift @IO@ into @m@:
--
-- @
-- handleRequest :: AppEnv -> UserId -> IO Feed
-- handleRequest env uid =
--   runAppM env $ runFetch (runAppM env) liftIO (getUserFeed uid)
-- @
--
-- For monads that deliberately avoid 'MonadIO' (e.g. a @Transaction@
-- type), export a convenience runner that hides the unsafe nats:
--
-- @
-- fetchInTransaction :: Fetch Transaction a -> Transaction a
-- fetchInTransaction = runFetch unsafeRunTransaction unsafeLiftIO
-- @
--
-- == Step 5: Test it
--
-- Use 'MockFetch' to run the same code against canned data, with no IO,
-- no database, and no cache:
--
-- @
-- testGetUserFeed :: IO ()
-- testGetUserFeed = do
--   let mocks = mockData \@UserId      [(UserId 1, testUser)]
--            <> mockData \@PostsByAuthor [(PostsByAuthor 1, [testPost])]
--   feed <- runMockFetch \@AppM mocks (getUserFeed (UserId 1))
--   assertEqual (feedUser feed) testUser
-- @
--
-- Because @getUserFeed@ is polymorphic in @n@, no code changes are needed
-- to swap between 'Fetch' (production) and 'MockFetch' (tests).
--
-- = Error handling
--
-- If 'batchFetch' throws for a subset of keys, the engine fills unfilled
-- entries with the exception. Callers using 'fetch' see the exception
-- re-thrown; callers using 'tryFetch' receive @Left SomeException@.
-- Failures for one key do not affect other keys in the same batch.
--
-- All monad transformers ('Fetch', 'TracedFetch', 'Mutate',
-- 'MockFetch', 'MockMutate') provide @MonadThrow@ and @MonadCatch@
-- instances from the @exceptions@ package. The 'MonadCatch' instance
-- on 'Fetch' propagates the handler through 'Blocked' continuations,
-- so a @catch@ wrapping a multi-round computation catches exceptions
-- thrown in any round, not just the initial probe.
--
-- @
-- import "Control.Monad.Catch" ('Control.Monad.Catch.catch', 'Control.Monad.Catch.throwM')
--
-- safeFetch :: ('MonadFetch' m n, 'Control.Monad.Catch.MonadCatch' n, DataSource m k, Typeable (Result k))
--           => k -> Result k -> n (Result k)
-- safeFetch k fallback =
--   'Control.Monad.Catch.catch' (fetch k) (\\(_ :: SomeException) -> pure fallback)
-- @
--
-- @MonadMask@ is intentionally not provided: async exception masking
-- across batch round boundaries is not well-defined.
--
-- = Further reading
--
--   * 'FetchStrategy': control whether a source runs concurrently,
--     sequentially, or with eager start.
--   * 'CachePolicy': opt out of caching for mutation-like sources.
--   * 'TracedFetch': round-by-round observability hooks.
--   * 'runLoopWith': build custom instrumented runners (e.g. for
--     OpenTelemetry) by wrapping around each batch round.
--   * 'MemoStore': cache derived computations (not just raw fetches).
--   * The @docs/DESIGN.md@ in the repository covers the full set of design
--     decisions and tradeoffs relative to Haxl.
module Fetch
  ( -- * Defining data sources
    -- | Start here. A 'FetchKey' pairs a key type with its result type;
    -- a 'DataSource' teaches the engine how to batch-fetch those keys.
    FetchKey(..)
  , DataSource(..)
  , FetchStrategy(..)
  , CachePolicy(..)

    -- * Fetching data
    -- | The interface your application code programs against.
    -- Use 'fetch' to request a single key, 'tryFetch' for explicit error
    -- handling, and the combinators below for collections.
  , MonadFetch(..)
  , fetchAll
  , fetchWith
  , fetchThrough
  , fetchMap
  , fetchMaybe
  , fetchMapWith

    -- * Running
    -- | Execute a 'MonadFetch' computation via 'FetchConfig'.
  , Fetch
  , FetchConfig(..)
  , fetchConfig
  , fetchConfigIO
  , liftSource
  , runFetch
  , runFetch'

    -- * Testing
    -- | Swap 'Fetch' for 'MockFetch' to run the same polymorphic code
    -- against canned data: no IO, no database, no cache.
  , MockFetch
  , runMockFetch
  , ResultMap
  , mockData
  , emptyMockData

    -- * Mutations
    -- | Mutations model write operations: creating a row, publishing a
    -- message, calling a side-effecting RPC. Unlike fetches, mutations are
    -- never batched, deduplicated, or cached: each 'mutate' call executes
    -- exactly once, in order.
    --
    -- 'Mutate' layers on top of 'Fetch'. A computation alternates between
    -- __fetch phases__ (where reads batch normally via 'Applicative') and
    -- __mutation steps__ (where writes run sequentially). After each
    -- mutation, 'reconcileCache' lets you evict stale entries or warm
    -- fresh data so that subsequent fetches see the updated state.
    --
    -- __Caveat:__ by mixing reads and writes in the same computation, you
    -- take on the responsibility of keeping the fetch cache coherent.
    -- The engine cannot know which cached entries a mutation invalidates;
    -- that is domain knowledge only you have. If you forget to evict or
    -- re-warm a stale entry in 'reconcileCache', subsequent fetches will
    -- silently return the old value. For many applications the simpler
    -- approach is to keep mutations in plain @IO@ and use 'Fetch' only
    -- for the read path; 'Mutate' is there for cases where interleaved
    -- read-after-write within a single computation is genuinely needed.

    -- ** Defining mutations
  , MutationKey(..)
  , MutationSource(..)

    -- ** Running mutations
  , MonadMutate(..)
  , Mutate
  , runMutate
  , liftFetch

    -- ** Testing mutations
  , MockMutate
  , runMockMutate
  , MutationHandlers
  , mockMutation
  , emptyMutationHandlers
  , RecordedMutation(..)

    -- * Cache management
    -- | Most users never touch the cache directly; the engine manages it.
    -- These are useful for pre-warming from an external store, selective
    -- eviction after mutations, or sharing a cache across sequential phases.
  , CacheRef
  , newCacheRef
  , CacheLookup(..)
  , cacheLookup
  , cacheInsert
  , cacheInsertError
  , cacheEvict
  , cacheEvictSource
  , cacheEvictWhere
  , cacheWarm
  , cacheContents

    -- * Tracing and observability
    -- | 'TracedFetch' is a turnkey wrapper with per-round callbacks.
    -- For richer instrumentation (e.g. OpenTelemetry spans), build a
    -- custom runner using the extension API below.
  , TracedFetch
  , TraceConfig(..)
  , defaultTraceConfig
  , FetchStats(..)
  , runTracedFetch

    -- * Memoization
    -- | Cache derived computations (not just raw fetches) within a request.
  , MemoKey(..)
  , MemoStore
  , newMemoStore
  , memo
  , memoOn

    -- * Errors
  , FetchError(..)

    -- * Extension API
    -- | Building blocks for custom runners and instrumentation.
    -- Application code does not need anything from this section.
    --
    -- The simplest way to add instrumentation is 'runLoopWith', which
    -- lets you wrap each batch round with before\/after logic (e.g.
    -- opening and closing a tracing span):
    --
    -- @
    -- import Fetch.Batched ('FetchEnv'(..), 'runLoopWith')
    -- import Fetch.Engine  ('RoundStats'(..))
    --
    -- myInstrumentedRunner :: Monad m
    --                      => (forall x. m x -> IO x)
    --                      -> (forall x. IO x -> m x)
    --                      -> Fetch m a -> m a
    -- myInstrumentedRunner lower lift action = do
    --   cRef <- lift 'newCacheRef'
    --   let e = 'FetchEnv' cRef lower lift
    --   'runLoopWith' e (\\n batches exec -> do
    --       -- before round
    --       stats <- exec
    --       -- after round, stats :: 'RoundStats'
    --       pure ()
    --     ) action
    -- @
    --
    -- For full control (e.g. running entirely in @IO@ with a single
    -- @lift@ at the boundary), use 'Fetch'\'s constructor, 'FetchEnv',
    -- and 'executeBatches' directly.

    -- ** Loop helpers
  , FetchEnv(..)
  , runLoop
  , runLoopWith
  , RoundStats(..)
  , emptyRoundStats

    -- ** Batch inspection
  , MonadFetchBatch(..)
  , Status(..)
  , Batches(..)
  , batchSize
  , batchSourceCount

    -- ** Engine
  , executeBatches

    -- * Instance helpers
    -- | Combinators for implementing 'DataSource' from simpler primitives.
    -- See also 'fetchOne' (a default method on 'DataSource') and the
    -- "Fetch.Deriving" module for DerivingVia patterns.
  , optionalBatchFetch
  , traverseBatchFetch

    -- * Re-exports
  , Typeable
  , Hashable
  , NonEmpty(..)
  , Proxy(..)
  ) where

import Fetch.Class
import Fetch.Batched
  ( Fetch, FetchConfig(..), fetchConfig, fetchConfigIO, FetchEnv(..), liftSource
  , runFetch, runFetch'
  , runLoop, runLoopWith
  )
import Fetch.Cache
  ( CacheRef, newCacheRef
  , CacheLookup(..), cacheLookup
  , cacheInsert, cacheInsertError
  , cacheEvict, cacheEvictSource, cacheEvictWhere
  , cacheWarm, cacheContents
  )
import Fetch.Combinators
import Fetch.Deriving (optionalBatchFetch, traverseBatchFetch)
import Fetch.Engine (RoundStats(..), emptyRoundStats, executeBatches)
import Fetch.Mutate
  ( MutationSource(..), MonadMutate(..)
  , Mutate, runMutate, liftFetch
  )
import Fetch.Mock
  ( MockFetch, runMockFetch, ResultMap, mockData, emptyMockData
  , MockMutate, runMockMutate, MutationHandlers, mockMutation
  , emptyMutationHandlers, RecordedMutation(..)
  )
import Fetch.Traced (TracedFetch, TraceConfig(..), defaultTraceConfig, FetchStats(..), runTracedFetch)
import Fetch.Memo (MemoKey(..), MemoStore, newMemoStore, memo, memoOn)
import Fetch.IVar (FetchError(..))
