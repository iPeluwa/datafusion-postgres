# AGENT.md - DataFusion Postgres

## Build/Lint/Test Commands
- **Build**: `cargo build` (workspace root) or `cargo build --all-features`
- **Test**: `cargo test --all-features` (runs all unit tests)
- **Single test**: `cargo test --package datafusion-postgres test_name` or `cargo test --package datafusion-postgres-cli test_name`
- **Lint**: `cargo clippy --all-features -- -D warnings`
- **Format**: `cargo fmt -- --check` (check) or `cargo fmt` (apply)
- **Integration tests**: `./tests-integration/test.sh` (requires Python with psycopg)
- **MSRV check**: Use Rust 1.82.0 minimum

## Architecture & Structure
- **Workspace**: 2 main crates: `datafusion-postgres` (library) and `datafusion-postgres-cli` (binary)
- **Library (`datafusion-postgres`)**: Exposes `serve()` function to serve DataFusion SessionContext via Postgres wire protocol
- **CLI**: Serves CSV/JSON/Arrow/Parquet/Avro files as Postgres-queryable tables
- **Integration**: Uses `pgwire` crate for Postgres protocol, `datafusion` for query engine
- **Test data**: `tests-integration/` contains test files and Python-based integration tests

## Code Style & Conventions
- **Edition**: Rust 2021, MSRV 1.82.0
- **Imports**: Standard library first (`use std::`), then external crates, then local modules
- **Error handling**: Use `PgWireResult` for wire protocol errors, standard Result elsewhere  
- **Async**: Tokio-based async with `async-trait` for trait implementations
- **Formatting**: Standard rustfmt, enforced in CI
- **Dependencies**: Workspace-level dependency management, use `workspace = true` in sub-crates
