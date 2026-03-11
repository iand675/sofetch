# Changelog for `sofetch`

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the
[Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

## 0.1.0.3 — 2026-03-11

### Fixed

- Fixed test suite deadlock caused by a tight spin loop in the
  `"primeCache into pending IVar while batch in flight"` race condition
  test. The loop polled `cacheLookup` via `readIORef` without yielding,
  starving the forked thread on ARM64 and blocking async exception
  delivery. Added `yield` to break the spin.

### Changed

- Wrapped ~40 concurrency-prone tests with a `timed` combinator that
  enforces a 5-second deadline, so future deadlocks fail fast instead
  of hanging the suite.
- Updated package metadata (author, maintainer, category, synopsis).

## 0.1.0.2

- Internal version bump (metadata only).

## 0.1.0.0

- Initial release.
