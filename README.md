<p align="center">
  <img src="logo.svg" alt="sofetch" width="400">
</p>

<p align="center">
  <img src="fetch.gif" alt="That's so fetch" width="300">
</p>

---

Automatic batching and deduplication of concurrent data fetches for Haskell.

Write sequential-looking code; get batched, deduplicated, concurrent data
access. Inspired by Facebook's
[Haxl](https://github.com/facebook/Haxl) (Marlow et al., ICFP 2014), with a
simpler API and monad-transformer design.

## Key features

- **No GADTs in user code.** Data sources are ordinary typeclasses with
  associated type families. Key types use stock `deriving`.
- **Data sources run in your monad.** `DataSource` is parameterised by a monad
  `m`, not a concrete environment. If `m` is `ReaderT AppEnv IO`, your sources
  have access to connection pools, config, etc. Missing instances are
  compile-time errors.
- **Monad transformer.** `Fetch m a` layers over your source monad. Two
  natural transformations (`m -> IO` and `IO -> m`) bridge the gap at the run
  site.
- **Swappable implementations.** `MonadFetch m n` is the application-facing
  typeclass. Production (`Fetch`), traced (`TracedFetch`), and mock
  (`MockFetch`) all satisfy it.
- **Extensible instrumentation.** The core provides `runLoopWith` for wrapping
  each batch round (e.g. with tracing spans). OpenTelemetry support lives in
  the separate [`sofetch-otel`](./sofetch-otel) package.

## Quick start

### 1. Define key types

Each type of data you fetch gets its own key type with a `FetchKey` instance
that declares the result type. `FetchKey` requires `Typeable`, `Hashable`,
`Eq`, and `Show` -- all derivable with stock or anyclass strategies:

```haskell
{-# LANGUAGE DeriveGeneric, DeriveAnyClass, DerivingStrategies, TypeFamilies #-}

data User = User { userId :: Int, userName :: Text }
data Post = Post { postId :: Int, postAuthorId :: Int, postTitle :: Text }

newtype UserId = UserId Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey UserId where
  type Result UserId = User

newtype PostsByAuthor = PostsByAuthor Int
  deriving stock (Eq, Ord, Show, Generic)
  deriving anyclass (Hashable)

instance FetchKey PostsByAuthor where
  type Result PostsByAuthor = [Post]
```

### 2. Define data sources

A `DataSource` instance teaches the engine how to batch-fetch keys. The
monad `m` provides any resources the source needs (connection pool, config,
etc.). `batchFetch` receives a `NonEmpty k` and must return a `HashMap k
(Result k)` with an entry for every key:

```haskell
newtype AppM a = AppM (ReaderT AppEnv IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadUnliftIO, MonadReader AppEnv)

instance DataSource AppM UserId where
  batchFetch keys = do
    pool <- asks appPool
    let ids = [uid | UserId uid <- toList keys]
    rows <- liftIO $ withResource pool $ \conn ->
      query conn "SELECT id, name FROM users WHERE id = ANY(?)" (Only ids)
    pure $ HM.fromList [(UserId (userId u), u) | u <- rows]

instance DataSource AppM PostsByAuthor where
  batchFetch keys = do
    pool <- asks appPool
    let ids = [aid | PostsByAuthor aid <- toList keys]
    rows <- liftIO $ withResource pool $ \conn ->
      query conn "SELECT id, author_id, title FROM posts WHERE author_id = ANY(?)" (Only ids)
    let grouped = HM.fromListWith (++) [(PostsByAuthor (postAuthorId p), [p]) | p <- rows]
    pure $ HM.union grouped (HM.fromList [(k, []) | k <- toList keys])
```

If your data source has no native batch API, implement `fetchOne` instead --
the default `batchFetch` calls it for each key:

```haskell
instance DataSource AppM UserId where
  fetchOne (UserId uid) = lookupUserById uid
```

### 3. Write data-access code

Program against `MonadFetch` -- don't commit to an implementation:

```haskell
getUserFeed :: (MonadFetch m n, DataSource m UserId, DataSource m PostsByAuthor)
            => Int -> n (User, [Post])
getUserFeed uid =
  (,) <$> fetch (UserId uid) <*> fetch (PostsByAuthor uid)
```

You can also use `ApplicativeDo` if you prefer `do`-notation:

```haskell
{-# LANGUAGE ApplicativeDo #-}

getUserFeed uid = do
  user  <- fetch (UserId uid)         -- batched together
  posts <- fetch (PostsByAuthor uid)  -- in one round
  pure (user, posts)
```

Both forms produce the same batching behaviour -- `ApplicativeDo` is a
convenience, not a requirement.

### 4. Run it

If your source monad has a `MonadUnliftIO` instance (true for any
`ReaderT env IO` stack), use `fetchConfigIO` to build the config -- no
manual natural transformations needed:

```haskell
handleRequest :: AppEnv -> Int -> IO (User, [Post])
handleRequest env uid = runAppM env $ do
  cfg <- fetchConfigIO
  runFetch cfg (getUserFeed uid)
```

To preserve the cache across sequential phases, use `runFetch'` which
returns the `CacheRef` alongside the result:

```haskell
handleTwoPhases :: AppEnv -> IO [Post]
handleTwoPhases env = runAppM env $ do
  cfg <- fetchConfigIO

  -- Phase 1: populate cache
  (_users, cache) <- runFetch' cfg $
    fetchAll [UserId 1, UserId 2, UserId 3]

  -- Phase 2: reuse the warm cache — cached keys resolve without hitting the DB
  runFetch cfg { configCache = Just cache } $
    fetchAll [PostsByAuthor 1, PostsByAuthor 2]
```

You can also create a `CacheRef` up front with `newCacheRef` and thread it
through multiple `runFetch` calls via `cfg { configCache = Just cRef }`.

#### Monads without `MonadUnliftIO`

For monads that deliberately avoid `MonadIO` (e.g. a `Transaction` type
that prevents arbitrary IO inside database transactions), use `fetchConfig`
with explicit natural transformations and export a convenience runner that
hides the unsafe nats:

```haskell
fetchInTransaction :: Fetch Transaction a -> Transaction a
fetchInTransaction = runFetch (fetchConfig unsafeRunTransaction unsafeLiftIO)
```

The two natural transformations (`unsafeRunTransaction` and `unsafeLiftIO`)
stay private to your DB module. Application code calls `fetchInTransaction`
and never touches IO. The engine uses the nats internally for cache and IVar
operations, but the public interface is completely safe.

See `examples/SqliteBlog.hs` (scenario 12) for a worked proof-of-concept with
a `DB` monad that has no `MonadIO` instance.

### 5. Test it

Swap to `MockFetch` with canned data -- no IO, no database:

```haskell
testGetUserFeed :: IO ()
testGetUserFeed = do
  let mocks = mockData @UserId        [(UserId 1, testUser)]
           <> mockData @PostsByAuthor  [(PostsByAuthor 1, [testPost])]
  (user, posts) <- runMockFetch @AppM mocks (getUserFeed 1)
  assertEqual user testUser
  assertEqual posts [testPost]
```

## Examples

The `examples/` directory contains two runnable programs that demonstrate
sofetch against real-world backends. Build them with:

```bash
stack build --flag sofetch:examples
stack exec sqlite-blog
stack exec github-explorer
```

### SQLite blog platform (`examples/SqliteBlog.hs`)

A blog platform with users, posts, and comments backed by an in-memory SQLite
database. Every `batchFetch` prints the SQL it executes, and an instrumented
runner shows round boundaries, key counts, and cache behaviour.

Scenarios include:

- **Applicative batching** -- two data sources dispatched concurrently in one round
- **N+1 avoidance** -- fan-out from posts to comments in a single `WHERE IN` query
- **Deduplication** -- overlapping author references across comments, fetched once
- **Deep N+1 across function boundaries** -- four layers of independently-written
  functions (`renderBlogPage` → `renderAuthorProfile` → `renderPostWithComments`
  → `renderComment`), each fetching its own data. `traverse` merges all fetches
  at the same depth into one round. Three authors × seven posts × twelve comments
  collapses from 25+ queries to 4.
- **Faceted queries** -- assembling search result cards where each card needs
  four independent facets (post, author, comment count, latest comment). Five
  cards × four facets = 20 potential queries; sofetch does it in 2 rounds / 5
  SQL queries.
- **Chunked batching** -- a `DataSource` that splits large `IN` clauses into
  chunks of 50, transparent to the caller.
- **Shared cache** -- two separate computations sharing a `CacheRef`, proving
  that the second run resolves cached keys without hitting the database.
- **MockFetch** -- the same polymorphic `getUserFeed` function run against both
  real SQLite and canned mock data with zero code changes.
- **Restricted DB monad** -- a `DB` type with no `MonadIO` instance
  (mimicking Mercury's transaction monad), showing the
  `fetchInTransaction`-style pattern where the unsafe natural transformations
  are hidden behind a safe public runner.

### GitHub API explorer (`examples/GitHubExplorer.hs`)

Concurrent exploration of the GitHub REST API using `http-client-tls` and
`aeson`. Demonstrates sofetch with HTTP backends where the value is
concurrency, deduplication, and caching rather than SQL batching.

Scenarios include concurrent fan-out, dedup + caching (same user from two code
paths → one HTTP request), error handling with `tryFetch`, and combinators.

## How batching works

`Fetch` has an `Applicative` instance that merges pending fetches from both
sides of `<*>` into a single round. The `Monad` instance introduces a round
boundary at `>>=` (the right side depends on the left side's result).

You can express independent fetches directly with `<*>` (or `liftA2`, etc.),
or enable `ApplicativeDo` and let GHC desugar `do`-blocks into `Applicative`
combinators wherever data dependencies allow. Either way, independent fetches
batch automatically.

Within each round, keys for the same data source are grouped into one
`batchFetch` call. Keys for different sources run concurrently by default
(configurable via `FetchStrategy`). Duplicate keys are deduplicated across the
entire computation.

## Packages

| Package | Description |
|---|---|
| **sofetch** | Core library: `Fetch`, `DataSource`, `MonadFetch`, cache, engine, mocks, tracing hooks |
| **[sofetch-otel](./sofetch-otel)** | OpenTelemetry instrumentation via `runFetchWithOTel` |

## Extension API

The core exports building blocks for custom instrumented runners:

- `runLoopWith` -- wrap each batch round with before/after logic
- `FetchEnv(..)` -- construct the internal environment
- `executeBatches` / `RoundStats(..)` -- run batches directly
- `Status(..)` / `Batches(..)` -- inspect fetch state

See the [Extension API](./src/Fetch.hs) section in the module docs and
[`sofetch-otel`](./sofetch-otel) for a worked example.

## Modules

| Module | Contents |
|---|---|
| `Fetch` | Top-level re-exports |
| `Fetch.Class` | `FetchKey`, `DataSource`, `MonadFetch`, `MonadFetchBatch`, `Status`, `Batches` |
| `Fetch.Batched` | `Fetch` monad transformer, runners, `runLoopWith` |
| `Fetch.Engine` | Batch dispatch with strategy-based scheduling |
| `Fetch.Cache` | IVar-based cache with dedup, eviction, warming |
| `Fetch.IVar` | Write-once variable with error support |
| `Fetch.Combinators` | `fetchAll`, `fetchThrough`, `fetchMap`, etc. |
| `Fetch.Mock` | `MockFetch` for testing |
| `Fetch.Traced` | `TracedFetch` with per-round callbacks |
| `Fetch.Mutate` | `Mutate` for interleaved read-write computations |
| `Fetch.Memo` | `MemoStore`, `memo`, `memoOn` |
| `Fetch.Deriving` | Helpers for writing instances (`optionalBatchFetch`, DerivingVia docs) |

## Design

See [docs/DESIGN.md](./docs/DESIGN.md) for the full set of design decisions
and tradeoffs relative to Haxl.
