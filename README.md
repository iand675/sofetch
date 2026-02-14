<p align="center">
  <img src="logo.svg" alt="sofetch" width="400">
</p>

<p align="center">
  <img src="https://media.giphy.com/media/xT9KVF4zNt70nyNpi8/giphy.gif" alt="That's so fetch" width="300">
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
- **Monad transformer.** `FetchT m a` layers over your source monad. Two
  natural transformations (`m -> IO` and `IO -> m`) bridge the gap at the run
  site.
- **Swappable implementations.** `MonadFetch m n` is the application-facing
  typeclass. Production (`FetchT`), traced (`TracedFetchT`), and mock
  (`MockFetchT`) all satisfy it.
- **Extensible instrumentation.** The core provides `runLoopWith` for wrapping
  each batch round (e.g. with tracing spans). OpenTelemetry support lives in
  the separate [`sofetch-otel`](./sofetch-otel) package.

## Quick start

### 1. Define key types

```haskell
newtype UserId = UserId Int
  deriving (Eq, Hashable, Show)

instance FetchKey UserId where
  type Result UserId = User
```

### 2. Define data sources

```haskell
newtype AppM a = AppM (ReaderT AppEnv IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader AppEnv)

instance DataSource AppM UserId where
  batchFetch ids = do
    pool <- asks appPool
    liftIO $ withResource pool $ \conn ->
      query conn "SELECT id, name FROM users WHERE id = ANY(?)" (Only ids)
```

### 3. Write data-access code

Program against `MonadFetch` -- don't commit to an implementation:

```haskell
getUserFeed :: (MonadFetch m n, DataSource m UserId, DataSource m PostsByAuthor)
            => UserId -> n Feed
getUserFeed uid =
  Feed <$> fetch uid <*> fetch (PostsByAuthor uid)
```

You can also use `ApplicativeDo` if you prefer `do`-notation:

```haskell
{-# LANGUAGE ApplicativeDo #-}

getUserFeed uid = do
  user  <- fetch uid                  -- batched together
  posts <- fetch (PostsByAuthor uid)  -- in one round
  pure (Feed user posts)
```

Both forms produce the same batching behaviour -- `ApplicativeDo` is a
convenience, not a requirement.

### 4. Run it

```haskell
handleRequest :: AppEnv -> UserId -> IO Feed
handleRequest env uid =
  runAppM env $ runFetchT (runAppM env) liftIO (getUserFeed uid)
```

For monads that deliberately avoid `MonadIO` (e.g. a `Transaction` type),
export a convenience runner that hides the unsafe nats:

```haskell
fetchInTransaction :: FetchT Transaction a -> Transaction a
fetchInTransaction = runFetchT unsafeRunTransaction unsafeLiftIO
```

### 5. Test it

Swap to `MockFetchT` with canned data -- no IO, no database:

```haskell
testGetUserFeed :: IO ()
testGetUserFeed = do
  let mocks = mockData @UserId      [(UserId 1, testUser)]
           <> mockData @PostsByAuthor [(PostsByAuthor 1, [testPost])]
  feed <- runMockFetchT @AppM mocks (getUserFeed (UserId 1))
  assertEqual (feedUser feed) testUser
```

## How batching works

`FetchT` has an `Applicative` instance that merges pending fetches from both
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
| **sofetch** | Core library: `FetchT`, `DataSource`, `MonadFetch`, cache, engine, mocks, tracing hooks |
| **[sofetch-otel](./sofetch-otel)** | OpenTelemetry instrumentation via `runFetchTWithOTel` |

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
| `Fetch.Batched` | `FetchT` monad transformer, runners, `runLoopWith` |
| `Fetch.Engine` | Batch dispatch with strategy-based scheduling |
| `Fetch.Cache` | IVar-based cache with dedup, eviction, warming |
| `Fetch.IVar` | Write-once variable with error support |
| `Fetch.Combinators` | `fetchAll`, `fetchThrough`, `fetchMap`, etc. |
| `Fetch.Mock` | `MockFetchT` for testing |
| `Fetch.Traced` | `TracedFetchT` with per-round callbacks |
| `Fetch.Mutate` | `MutateT` for interleaved read-write computations |
| `Fetch.Memo` | `MemoStore`, `memo`, `memoOn` |
| `Fetch.Deriving` | Helpers for writing instances (`optionalBatchFetch`, DerivingVia docs) |

## Design

See [docs/DESIGN.md](./docs/DESIGN.md) for the full set of design decisions
and tradeoffs relative to Haxl.
