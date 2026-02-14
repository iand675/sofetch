# Fetch: A Simplified Haxl

## Motivation

Haxl solves a huge problem (automatic batching of concurrent data fetches), but its implementation is heavy. Defining a data source requires navigating `BlockedFetch`, `PerformFetch`, `ResultVar`, `StateKey`, `StateStore`, and a GADT indexed by the result type. In my experience this meant writing a lot of Template Haskell to stitch GADT-indexed results back together, and the "data source" abstraction itself didn't pull its weight: most of the time I just wanted to batch-fetch a list of typed keys, not wrangle existentials and state tokens.

Fetch keeps Haxl's core idea (write sequential-looking code, get batched data access) while simplifying the machinery.

---

## Core Design Decisions

### Replacing the GADT with a Type Family

**Haxl's approach:**

```haskell
data UserReq a where
  GetUser  :: UserId -> UserReq User
  GetPosts :: UserId -> UserReq [Post]
```

Every data source defines a GADT indexed by the result type. The GADT is what lets Haxl pair each request with its expected response type at the type level. This is sound but requires `GADTs`, `StandaloneDeriving`, custom `Eq`/`Hashable` instances (since deriving doesn't work well with GADTs), and a `DataSource` class with a `StateKey` associated type.

**Fetch's approach:**

```haskell
class (Typeable k, Hashable k, Eq k, Show k) => FetchKey k where
  type Result k

newtype UserId = UserId Int
  deriving (Eq, Hashable, Show, Typeable)

instance FetchKey UserId where
  type Result UserId = User
```

The request/response pairing moves from a GADT index to an associated type family. Each "request type" is an ordinary Haskell type with stock-derivable instances.

**Tradeoffs:**

- *Pro:* No GADTs in user-facing code. Stock `deriving` works. New data sources are two small class instances.
- *Pro:* Each request type is a proper first-class type: it can be used as a `Map` key, put in a `Set`, serialized, etc., without the ceremony that GADTs require.
- *Con:* A GADT can have multiple constructors with different result types under one data source (e.g., `GetUser :: UserId -> UserReq User` and `GetPosts :: UserId -> UserReq [Post]`). With `FetchKey`, each result type needs its own key type. In practice this is rarely a problem; most sources have one primary query pattern, and having separate types for `UserId` vs. `PostsByAuthor` is better domain modeling anyway (IMO).
- *Con:* The type family is open, so there's no exhaustiveness checking. You can't pattern match on "all possible fetches for this source." This matters for Haxl's internal profiling (which groups stats by data source) but doesn't affect application code.

---

### Monad-Parameterized Data Sources

**Haxl's approach:**

`StateKey` associates a mutable state type with each data source. States are stored in a `StateStore` (heterogeneous map backed by `Dynamic`) inside the `Env`. Data sources read and potentially modify their state during fetch execution. If you forget to initialize a source's state, the code compiles but throws a runtime error.

**Fetch's approach:**

`DataSource` takes the monad type as a parameter. The monad `m` is the source monad (the monad that `batchFetch` runs in). The environment (connection pools, config, etc.) is carried by `m` itself (e.g., `m` is `ReaderT AppEnv IO`):

```haskell
class (FetchKey k, Monad m) => DataSource m k where
  batchFetch :: [k] -> m (HashMap k (Result k))
```

The runner provides two natural transformations to bridge `m` with `IO`:

```haskell
type AppM = ReaderT AppEnv IO

instance DataSource AppM UserId where
  batchFetch ids = ask >>= \env ->
    withResource (appPool env) $ \conn ->
      query conn "SELECT * FROM users WHERE id = ANY(?)" (Only ids)

runFetch :: Monad m
          => (forall x. m x -> IO x)
          -> (forall x. IO x -> m x)
          -> Fetch m a
          -> m a
```

**Tradeoffs:**

- *Pro:* Compile-time safety. No possibility of runtime "missing config" errors.
- *Pro:* No `Dynamic`, no heterogeneous maps, no `Proxy` gymnastics at registration sites.
- *Pro:* The monad is explicit. You choose `m` to be whatever has the capabilities your sources need: `ReaderT AppEnv IO`, a test monad, etc.
- *Pro:* Dependencies are explicit in the type: `DataSource AppM UserId` says "to fetch users, you need an `AppM`" (which carries the env).
- *Con:* The `m` type appears in `MonadFetch m n`, `Fetch m`, and every `DataSource m k` instance. In practice this is one type, typically aliased, so the noise is manageable.
- *Con:* If you want different monads for different contexts (e.g., test vs. prod), you either parameterize your data access code (`DataSource m UserId => ...`) or define instances for both monads. Parameterizing adds constraints but is explicit about what sources a function uses.

---

### Applicative Batching with Monadic Sequencing

Both Haxl and Fetch use the same fundamental approach: `Applicative` merges blocked fetches from both sides of `<*>` into one round, `Monad` (`>>=`) is a round boundary. Neither speculates past a bind.

Fetch makes this explicit in the `Status` type:

```haskell
data Status m f a
  = Done a
  | Blocked (Batches m) (f a)
```

The key guarantee is that the run loop fully explores the entire computation tree before dispatching a round. The `<*>` instance probes both sides and merges their `Blocked` batches. The `>>=` instance probes the left side, and if it's `Done`, immediately probes the continuation too (so non-blocking monadic chains don't introduce unnecessary round boundaries). Only when the outermost probe returns `Blocked` does the engine dispatch, with every reachable fetch collected into a single `Batches`.

Concretely, in:

```haskell
(,,,) <$> fetch a <*> fetch b <*> (f <$> fetch c) <*> fetch d
```

a single probe walks all four branches, finds four blocked fetches, merges them, and dispatches one round. After that round fills the IVars, the continuations are re-probed and (since all four are now `Done`) the final value is produced with no further rounds.

With `ApplicativeDo` enabled, GHC desugars independent binds in `do` blocks into `<*>`, which covers the common case without the user needing to think about it.

---

### Transformer Instead of Concrete Monad

**Haxl's approach:**

`GenHaxl` is a concrete monad parameterized by a user environment type `u` and a write type `w`:

```haskell
newtype GenHaxl u w a = GenHaxl { ... }
```

All Haxl computation lives inside this monad. IO actions are lifted in via `liftIO`.

**Fetch's approach:**

`Fetch` is a monad transformer over the source monad `m`:

```haskell
newtype Fetch m a = Fetch
  { unFetch :: FetchEnv m -> m (Status m (Fetch m) a) }
```

The source monad `m` is arbitrary. Two natural transformations are provided at the runner: `lower :: m x -> IO x` to run source actions in IO (where the actual fetching happens), and `lift :: IO x -> m x` to lift IO into the source monad.

**Tradeoffs:**

- *Pro:* You can use any source monad: `ReaderT Config IO`, a test monad, a logging monad, etc.
- *Pro:* The natural transformation approach means `m` doesn't need a `MonadIO` constraint. You can use pure base monads in tests by providing nats that read from mock data.
- *Pro:* The environment is carried by `m` itself. Use `ReaderT AppEnv IO` as `m` if your sources need an environment.
- *Con:* Slightly more complex internal plumbing. The `FetchEnv` carries the cache ref and the two natural transformations.
- *Con:* Transformer stacking can interact poorly with the `Applicative` instance. Lifting `MonadFetch` through `StateT` requires care because the state threading can accidentally serialize applicative branches.

---

### Natural Transformation Instead of MonadIO

**Why not `MonadIO`:**

The obvious constraint for a monad that needs to do IO is `MonadIO m`. But this leaks into every type signature that uses `fetch`, forces test monads to provide a real `liftIO`, and makes it impossible to use genuinely pure base monads.

**The nat approach:**

```haskell
runFetch :: Monad m
          => (forall x. m x -> IO x)
          -> (forall x. IO x -> m x)
          -> Fetch m a
          -> m a
```

Two natural transformations are provided at the run site and captured in `FetchEnv`: `lower` runs source-monad actions in IO (for dispatching `batchFetch`), and `lift` lifts IO into the source monad (for cache operations). Application code using `MonadFetch` has no IO-related constraints at all.

**Tradeoffs:**

- *Pro:* Clean type signatures. `getUserFeed :: (MonadFetch m n, DataSource m UserId) => UserId -> n Feed` requires no `MonadIO`, no `MonadReader`, no constraints beyond what the function actually needs.
- *Pro:* Tests can provide mock nats. The nats are the single point of control for how IO is introduced.
- *Con:* Both nats must be provided at every `runFetch` call site. In production, `lower` is typically `runAppM env` (or similar) and `lift` is `liftIO`; in tests they can be trivial. This is effectively what MonadUnliftIO represents, but there
are cases where you have a monad with restricted IO capabilities (e.g. a transaction monad) that you want to run Fetch in,
so this lets you do that without opening the floodgates to arbitrary IO.

---

### Mutable Cache via IORef

The cache must be mutable and shared across all branches of an Applicative computation. Consider:

```haskell
(,) <$> branch1 <*> branch2
```

If both branches do multiple rounds internally, they need to see each other's cached results. A purely threaded cache (passing `ResultMap` as an argument and return value) doesn't work: the two branches would each get a snapshot and diverge.

An `IORef` is the simplest correct solution. The cache `IORef` is created by the runner and threaded through `FetchEnv`. The `lift` natural transformation bridges the `IO` reads/writes into the source monad.

**Tradeoffs:**

- *Pro:* Simple, correct, zero overhead compared to alternatives.
- *Con:* The `IORef` exists even when `m` is pure. In the `MockFetch` implementation this is irrelevant since it doesn't use the cache at all.

---

### IVars for In-Flight Deduplication

**The problem:**

Round N fires an async fetch for key A. Before it completes, round N+1 (from a different Applicative branch or a monadic continuation) also wants key A. Without deduplication, we'd fetch A twice.

**Haxl's approach:** `ResultVar` backed by `IORef (IVarContents a)` where `IVarContents` is either empty or full.

**Fetch's approach:** `IVar`, a write-once variable using `MVar` for blocking and `IORef` for non-blocking status checks:

```haskell
data IVar a = IVar
  { ivarResult :: !(MVar (Either SomeException a))
  , ivarFilled :: !(IORef Bool)
  }
```

The cache stores `HashMap k (IVar (Result k))` instead of `HashMap k (Result k)`. When a key is first requested, an IVar is allocated. Subsequent requests for the same key, even from concurrent rounds, get the same IVar and block on it until the fetch completes.

`cacheAllocate` is the dedup point: it atomically checks which keys already have IVars and only allocates new ones for genuinely new keys. This uses `atomicModifyIORef'` to avoid races.

**Tradeoffs:**

- *Pro:* Correct deduplication of in-flight requests with zero user-facing complexity. Users call `fetch` and the right thing happens.
- *Pro:* Per-key error handling: if a batch fetch succeeds for some keys and fails for others, each IVar can be independently filled with a value or error.
- *Con:* More complex cache internals. Every cache read now needs to distinguish three states (miss, pending, ready) instead of two (miss, hit).

---

### Type Class Interface with Swappable Implementations

**Haxl's approach:**

Everything goes through `GenHaxl`. There's one implementation.

**Fetch's approach:**

The interface is split into two type classes:

```haskell
class Monad n => MonadFetch m n | n -> m where
  fetch    :: (DataSource m k, Typeable (Result k)) => k -> n (Result k)
  tryFetch :: (DataSource m k, Typeable (Result k))
           => k -> n (Either SomeException (Result k))

class MonadFetch m n => MonadFetchBatch m n | n -> m where
  probe :: n a -> n (Status m n a)
  embed :: Status m n a -> n a
```

Here `m` is the source monad (where `DataSource` runs) and `n` is the fetch monad (where application code runs). The fundep `n -> m` means the source monad is determined by the fetch monad.

This split exists because application code never needs to inspect blocking structure, and lifting `MonadFetch` through standard transformers (`ReaderT`, `ExceptT`) is trivial (just `lift . fetch`). Lifting `MonadFetchBatch` is much harder and unnecessary outside of implementation code.

Multiple implementations:

| Implementation | Purpose |
|---|---|
| `Fetch m` | Production batching with caching and IVar dedup |
| `TracedFetch m` | Batching with round-by-round observability hooks |
| `MockFetch m n` | Pure, non-batching, reads from a pre-built result map (`m` phantom, `n` base) |

**Tradeoffs:**

- *Pro:* Test code doesn't need IO, async, or caching; just provide mock data.
- *Pro:* Instrumentation is a separate implementation, not flags on the main one.
- *Pro:* Application code is genuinely polymorphic. `getUserFeed :: (MonadFetch m n, DataSource m UserId) => UserId -> n Feed` can be run with any implementation.
- *Con:* Two type classes instead of one. The split needs to be explained and understood. In practice, most users only ever see `MonadFetch`.
- *Con:* The `MonadFetch` instance for transformers uses `lift`, which means fetches inside `StateT` don't carry state through the fetch. This is correct (fetches are side effects, not stateful computations) but can surprise people.

---

### Declarative Fetch Strategy Instead of User-Built FetchMode

**Haxl's approach:**

Data sources return a `PerformFetch` value that is either `SyncFetch`, `AsyncFetch`, or `BackgroundFetch`, each containing the actual IO action to execute. The data source author is responsible for constructing the right async machinery.

**Fetch's approach:**

Data sources implement `batchFetch` (or optionally `streamingFetch`) and declare a `FetchStrategy`:

```haskell
class (FetchKey k, Monad m) => DataSource m k where
  batchFetch :: [k] -> m (HashMap k (Result k))

  streamingFetch :: [k] -> (k -> Result k -> m ()) -> m ()
  -- Default delegates to batchFetch.

  fetchStrategy :: Proxy k -> FetchStrategy
  fetchStrategy _ = Concurrent

data FetchStrategy = Sequential | Concurrent | EagerStart
```

The engine handles all concurrency. `Concurrent` sources are dispatched with `async` and awaited together. `EagerStart` sources are dispatched first to overlap their latency with `Sequential` sources. The data source author never touches `Async`, `MVar`, or callbacks unless they opt into `streamingFetch`.

**Tradeoffs:**

- *Pro:* Simpler data source implementation. Most sources just implement `batchFetch` and accept the default `Concurrent` strategy.
- *Pro:* The engine controls all concurrency, so it can optimize scheduling, add timeouts, do backpressure, etc., without changing source implementations.
- *Pro:* `streamingFetch` is only needed for genuinely streaming sources (gRPC, websockets). The default implementation delegates to `batchFetch`.
- *Con:* Less flexibility. A source that wants fine-grained control over its async behavior (e.g., connection multiplexing, request pipelining) can't express that through the strategy enum. It would need to do this internally.

---

### Traversable-Aware Combinators

**The problem:**

Without combinators, fetching data for a collection requires destructuring, extracting keys, fetching, and rebuilding:

```haskell
enrichComments :: (MonadFetch m n, DataSource m CommentAuthor) => [Comment] -> n [(Comment, User)]
enrichComments comments = do
  let uids = map commentAuthor comments
  users <- traverse fetch uids
  pure (zip comments users)
```

This is boilerplate and fragile: the `zip` assumes the results come back in the same order as the keys, and more complex shapes (nested `Maybe`, maps, trees) make it worse.

**Fetch's approach:**

A set of combinators that preserve the shape of any `Traversable`:

```haskell
fetchAll     :: ... => t k -> n (t (Result k))
fetchWith    :: ... => t k -> n (t (k, Result k))
fetchThrough :: ... => (a -> k) -> t a -> n (t (a, Result k))
fetchMap     :: ... => (a -> k) -> (a -> Result k -> b) -> t a -> n (t b)
fetchMapWith :: ... => f k -> n (HashMap k (Result k))
```

All of these are implemented in terms of `traverse fetch`, which means `Fetch`'s `Applicative` instance automatically batches everything in a single round.

**Tradeoffs:**

- *Pro:* No manual destructure/reconstruct cycle. The combinator handles shape preservation.
- *Pro:* Zero runtime cost; these are just `traverse` under the hood.
- *Neutral:* These are convenience functions, not deep abstractions. A user who understands `traverse fetch` doesn't strictly need them. But they improve readability and reduce bugs in the "pair results back with inputs" step.

---

### Per-Key Error Handling

**Haxl's approach:**

`ResultVar` can hold either a result or an exception. If a fetch fails for one key, only that key's computation fails; other keys in the same batch can succeed independently.

**Fetch's approach:**

IVars serve the same role. Each IVar can be independently filled with `writeIVar` (success) or `writeIVarError` (failure). The `MonadFetch` class provides both throwing and non-throwing variants:

```haskell
class Monad n => MonadFetch m n | n -> m where
  fetch    :: (DataSource m k, Typeable (Result k)) => k -> n (Result k)
  tryFetch :: (DataSource m k, Typeable (Result k)) => k -> n (Either SomeException (Result k))
```

The engine wraps `batchFetch` in `try` and fills unfilled IVars with the caught exception. For `streamingFetch`, IVars that aren't filled after the streaming function returns are filled with a `FetchError`.

**Tradeoffs:**

- *Pro:* Partial batch failures don't kill unrelated fetches.
- *Pro:* Users choose their error handling style: `fetch` for the common case (let it throw), `tryFetch` when they want to handle failures explicitly.
- *Con:* `SomeException` as the error type is untyped. Users need to `catch` specific exception types. An associated `type FetchError k` was considered but rejected; it adds weight to `FetchKey` and most sources just want to throw and move on.

---

### Cache Eviction and Warming

**Haxl's approach:**

`dumpCacheAsHaskell` for debugging, limited eviction support.

**Fetch's approach:**

The cache is directly manipulable:

```haskell
cacheEvict       :: ... => CacheRef -> k -> IO ()
cacheEvictSource :: ... => CacheRef -> Proxy k -> IO ()
cacheEvictWhere  :: ... => CacheRef -> Proxy k -> (k -> Bool) -> IO ()
cacheWarm        :: ... => CacheRef -> HashMap k (Result k) -> IO ()
cacheContents    :: ... => CacheRef -> Proxy k -> IO (HashMap k (Result k))
```

`cacheWarm` creates pre-filled IVars for known values. This enables hydrating from an external cache (Redis, etc.) at request start, so subsequent `fetch` calls hit memory.

The `CacheRef` can be exported from `runFetchWithCache` and reused across multiple `runFetch` calls, enabling cache sharing across sequential phases of request processing.

**Tradeoffs:**

- *Pro:* Full control over cache lifecycle. The cache is a value you own, not an implementation detail hidden inside a monad.
- *Pro:* `cacheWarm` enables integration with external caching layers.
- *Con:* More API surface. For most use cases, the default "cache everything for the lifetime of the request" is sufficient and none of these functions are needed.

---

### CachePolicy on DataSource

Rather than a separate `uncachedFetch` backed by a newtype wrapper, caching behavior is declared on the data source itself:

```haskell
cachePolicy :: Proxy k -> CachePolicy
cachePolicy _ = CacheResults

data CachePolicy = CacheResults | NoCaching
```

Mutation sources set `NoCaching`. The engine checks the policy before cache reads and writes.

**Tradeoffs:**

- *Pro:* No newtype wrapping at call sites. `fetch (CreateUser userData)` works directly.
- *Pro:* The caching behavior is declared at the source definition, where it belongs.
- *Con:* Less flexible than per-call decisions. If you want the same source cached in some contexts and uncached in others, you need two key types.
- *Future:* `CachePolicy` could be extended to TTL-based expiry without changing the user-facing API.

---

### Memoization of Computations

**Haxl's approach:**

`memo` caches arbitrary computed results by a user-chosen key, avoiding redundant work when the same subcomputation runs multiple times in a request.

**Fetch's approach:**

A separate `MemoStore` with `memo` and `memoOn`:

```haskell
memo   :: (MemoKey k, ...) => MemoStore -> ... -> k -> m (MemoResult k) -> m (MemoResult k)
memoOn :: (Typeable a, ...) => MemoStore -> ... -> k -> m a -> m a
```

`memo` requires a `MemoKey` instance for type-safe keying. `memoOn` accepts any `Hashable` key inline using `Dynamic` internally, convenient for one-off memoization without declaring a new type.

**Tradeoffs:**

- *Pro:* Computation caching is separate from data source caching. Different lifetimes, different eviction policies, different concerns.
- *Con:* Requires passing the `MemoStore` and nat explicitly. This could be improved by folding `MemoStore` into `FetchEnv` and adding `memoize` to `MonadFetch`, at the cost of coupling the two concepts.

---

## Summary Table

| Aspect | Haxl | Fetch | Rationale |
|---|---|---|---|
| Request types | GADT per source | `FetchKey` + type family | Eliminates GADTs from user code |
| Source config | `StateKey` + `StateStore` + runtime lookup | Monad `m` on `DataSource m k` | Compile-time safety, no `Dynamic` |
| Source definition | `DataSource` + `StateKey` + `BlockedFetch` | `DataSource m k` + `batchFetch` | One function instead of five concepts |
| Monad | Concrete `GenHaxl u w` | `Fetch m` transformer | Composable with existing stacks |
| IO bridge | `MonadIO` / `liftIO` | Two natural transformations at run site | Cleaner constraints, testable |
| Concurrency | User-built `PerformFetch` | Declarative `FetchStrategy` | Engine handles async |
| Interface | Concrete monad | `MonadFetch m n` type class | Swappable implementations |
| In-flight dedup | `ResultVar` + `IVar` | `IVar` + `cacheAllocate` | Same semantics, cleaner API |
| Error handling | `ResultVar` exceptions | `IVar` exceptions + `tryFetch` | Per-key errors with explicit opt-in |
| Cache | Limited export | Full eviction, warming, export | Cache as first-class value |
| Profiling | Built into `GenHaxl` | Separate `TracedFetch m` | Separation of concerns |
| Memoization | `memo` inside `GenHaxl` | Separate `MemoStore` | Decoupled from fetch cache |
| Collection ops | Manual `traverse` | `fetchAll`, `fetchThrough`, `fetchMap` | Shape-preserving convenience |

---

## What We Intentionally Don't Do

**No built-in write type.** Haxl's `GenHaxl u w` has a write parameter `w` for accumulating side-channel data (like analytics events) during computation. Fetch leaves this to the base monad; use an IORef that you modify, or `WriterT` or `StateT` if you need it.

**No `dumpCacheAsHaskell`.** Haxl can serialize its cache as Haskell source code for replay/debugging. This is niche and tightly coupled to the GADT representation. `cacheContents` provides raw access; serialization is left to the user.

**No `Has`-style capability decomposition.** With monad-parameterized sources, `m` carries whatever capabilities your sources need. A source that only needs a `ConnectionPool` can use a monad that provides one (e.g., `ReaderT (ConnectionPool, ...) IO`). You can refine with a `Has`-style class if you want fine-grained dependency tracking, but we don't bake that in.

---

## Recommended Usage

Enable `ApplicativeDo` in all modules that use `fetch`. Write data access code against `MonadFetch`. Define data sources with `FetchKey` + `DataSource`. Call `runFetch` with two natural transformations.

```haskell
{-# LANGUAGE ApplicativeDo #-}

data AppEnv = AppEnv
  { appPool :: ConnectionPool
  }

type AppM = ReaderT AppEnv IO

instance FetchKey UserId where
  type Result UserId = User

instance DataSource AppM UserId where
  batchFetch ids = ask >>= \env ->
    withResource (appPool env) $ \conn ->
      query conn "SELECT * FROM users WHERE id = ANY(?)" (Only ids)

getUserFeed :: (MonadFetch m n, DataSource m UserId, DataSource m PostsByAuthor)
            => UserId -> n Feed
getUserFeed uid = do
  user  <- fetch uid
  posts <- fetch (PostsByAuthor uid)
  pure (Feed user posts)

runAppM :: AppEnv -> AppM a -> IO a
runAppM env = flip runReaderT env

handleRequest :: AppEnv -> UserId -> IO Feed
handleRequest env uid =
  runAppM env $ runFetch (runAppM env) liftIO (getUserFeed uid)
```

In tests:

```haskell
testGetUserFeed :: IO ()
testGetUserFeed = do
  let mocks = mockData @UserId [(UserId 1, testUser)]
           <> mockData @PostsByAuthor [(PostsByAuthor 1, [testPost])]
  feed <- runMockFetch @AppM mocks (getUserFeed (UserId 1))
  assertEqual (feedUser feed) testUser
```

---

## Module Structure

| Module | Contents |
|---|---|
| `Fetch.Class` | `FetchKey`, `DataSource`, `MonadFetch`, `MonadFetchBatch`, `Status`, `Batches` |
| `Fetch.IVar` | Write-once variable with error support |
| `Fetch.Cache` | IVar-based cache with dedup, eviction, warming |
| `Fetch.Engine` | Batch dispatch with strategy-based scheduling |
| `Fetch.Batched` | `Fetch`, `FetchEnv`, `runFetch`, `runFetchWithCache`, `runLoopWith`, `runLoop` |
| `Fetch.Combinators` | `fetchAll`, `fetchThrough`, `fetchMap`, etc. |
| `Fetch.Mock` | `MockFetch` and `mockData` for testing |
| `Fetch.Traced` | `TracedFetch` with per-round callbacks |
| `Fetch.Memo` | `MemoStore`, `memo`, `memoOn` |
| `Fetch.OpenTelemetry` | OpenTelemetry instrumentation; lives in the separate `sofetch-otel` package |
| `Fetch` | Top-level re-exports |
