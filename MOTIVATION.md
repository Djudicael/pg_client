# Motivation

## Why Another PostgreSQL Client?

You might ask — *why build yet another PostgreSQL driver when `sqlx`, `tokio-postgres`, and `diesel` already exist?* And you would be completely right to ask.

### The WASI Bet

For the past few years, I have been building all my applications with WASI (WebAssembly System Interface) in mind. This is not a passing trend. The cost of cloud infrastructure is high and only keeps rising — compute, memory, orchestration overhead. WASI promises something different: lightweight, sandboxed, portable components that start fast, consume little, and run anywhere — from a container to bare metal to the edge — without the baggage of a full OS dependency graph.

Cloud providers aren't going to suddenly make infrastructure cheaper. But what if your application footprint was 100x smaller? What if you didn't need to provision a heavyweight runtime per service? That is the bet.

### The Solo Developer Reality

I am a solo developer building a portfolio. I did not want to reduce my standards — I still wanted high performance, strong security, and real scalability — but I needed to be pragmatic. I wanted to deploy my projects without drowning in orchestration complexity.

Artist and developer — what a combo to never finish a side project. But the landscape has changed, and the WASI ecosystem has matured enough to make this viable.

### The Ecosystem Gap

Many WASI initiatives exist, but most do not contribute enough practical value back to the ecosystem (in my view). I wanted application and data-access logic to remain portable between conventional deployments and WASI components. The executable entry point and transport selection are necessarily target-specific, but the PostgreSQL client API and application logic should not require runtime-specific wrappers.

### The Hardest Problem: Database Connectivity

Classical applications rely heavily on OS-level libraries — filesystem, networking, system clocks — most of which are not available under WASI Preview 2. And even when WASI provides equivalents, the library ecosystem hasn't caught up: most Rust crates assume `std::net::TcpStream` or link against OpenSSL, neither of which compile to `wasm32-wasip2`.

This hit hardest with database connectivity. When this project began, I did not find a PostgreSQL client that matched its WASI target and runtime constraints. Established libraries such as `sqlx`, `tokio-postgres`, and `diesel` bring runtime and networking assumptions that did not fit that environment without substantial adaptation.

Even with Rust's ecosystem being more WASM-friendly than most languages, the modifications needed were enormous. Every dependency chain that touched the network layer — TLS, DNS, TCP, async I/O — had to be rewritten.

Eventually, I accepted that adapting an existing project was not practical. Starting from scratch was the faster path — and the better one, because it meant every design decision could be evaluated through the lens of WASI compatibility from day one.

### Building the Foundation

So I wrote `wasi-pg-client` — a pure-Rust, WASI-compatible PostgreSQL driver implementing the wire protocol directly. Every component was chosen for WASI compatibility:

- **Pure-Rust cryptography** — no OpenSSL, no system libraries, just `sha2`, `rustls`, and the RustCrypto ecosystem
- **WASI-native async I/O** — native WASI 0.3 async sockets and streams through `wasip3`, compiled with Rust's stable `wasm32-wasip2` target
- **Zero filesystem access** — TLS roots are embedded via `webpki-roots`, no cert files to read
- **One published crate** — protocol, type, pool, and transport layers are internal modules
- **Multi-target** — compiles to `wasm32-wasip2` and native, using WASI 0.3 imports on WASI and checking the Tier-3 `wasm32-wasip3` compiler target experimentally

It may not be the most feature-complete Postgres driver in the world, but it implements a broad client surface: parameterized queries, prepared statements with LRU caching, streaming results, transactions with savepoints, COPY protocol, LISTEN/NOTIFY, connection pooling, automatic reconnection with session state rebuild, and TLS with SCRAM authentication. Production suitability still depends on the runtime, deployment policy, and application-specific integration testing.

A note on connection pooling: the library ships a built-in pool behind the `pool` feature flag, but the supported WASI execution path has no background-task spawning API. A [PgBouncer](https://www.pgbouncer.org/) sidecar can therefore be a better production fit today: it sits outside the sandbox and manages connections across component instances. The in-process pool remains useful for native builds, cooperative async use, testing, and future runtime capabilities without making assumptions about when multithreaded component execution will be available.

### The Bottom Line

This project exists because:

1. Infrastructure costs are not going down, and WASI is the most credible path to reducing them without sacrificing capability.
2. No existing PostgreSQL client works under WASI without massive, invasive modifications — every OS-first library hits the same wall.
3. Building WASI-first from scratch produced a cleaner, more portable architecture than retrofitting would have.
4. The WASI ecosystem needs more practical, production-grade examples — not just "hello world" demos — to prove the model works.
5. As a portfolio project, it demonstrates deep understanding of networking protocols, async I/O, cryptography, and systems programming — all under the constraints of a sandboxed runtime.

If you are reading this and thinking about building for WASI: it is harder today, but the constraints force better design. And once you ship, you can deploy anywhere.
