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

Many WASI initiatives exist, but most do not contribute enough practical value back to the ecosystem (in my view). I wanted to build normal applications once — put them in containers on Kubernetes, Cloud Run, or whatever platform you choose, but also compile the exact same code to WASI and run it natively on a machine with **zero code changes**. No special SDK wrappers. No runtime-specific API surface.

### The Hardest Problem: Database Connectivity

Classical applications rely heavily on OS-level libraries — filesystem, networking, system clocks — most of which are not available under WASI Preview 2. And even when WASI provides equivalents, the library ecosystem hasn't caught up: most Rust crates assume `std::net::TcpStream` or link against OpenSSL, neither of which compile to `wasm32-wasip2`.

This hit hardest with database connectivity. I went all-in on PostgreSQL — but there was no PostgreSQL client library that worked under WASI. The existing libraries (`sqlx`, `tokio-postgres`, `diesel`) are all built on asynchronous runtimes and networking primitives that fundamentally depend on the host operating system. Adapting them would require rewriting every layer from the I/O primitives up.

Even with Rust's ecosystem being more WASM-friendly than most languages, the modifications needed were enormous. Every dependency chain that touched the network layer — TLS, DNS, TCP, async I/O — had to be rewritten.

Eventually, I accepted that adapting an existing project was not practical. Starting from scratch was the faster path — and the better one, because it meant every design decision could be evaluated through the lens of WASI compatibility from day one.

### Building the Foundation

So I wrote `wasi-pg-client` — a pure-Rust, WASI-compatible PostgreSQL driver implementing the wire protocol directly. Every component was chosen for WASI compatibility:

- **Pure-Rust cryptography** — no OpenSSL, no system libraries, just `sha2`, `rustls`, and the RustCrypto ecosystem
- **WASI-native async I/O** — using raw `wasip2` socket bindings via `wstd`, not libc sockets
- **Zero filesystem access** — TLS roots are embedded via `webpki-roots`, no cert files to read
- **Single crate** — no dependency sprawl, protocol and type systems are internal modules
- **Dual target** — compiles to both `wasm32-wasip2` and native with the same feature flags

It may not be the most feature-complete Postgres driver in the world, but it is complete enough for production workloads: parameterized queries, prepared statements with LRU caching, streaming results, transactions with savepoints, COPY protocol, LISTEN/NOTIFY, connection pooling, automatic reconnection with session state rebuild, and TLS with SCRAM authentication.

A note on connection pooling: the library ships a built-in pool behind the `pool` feature flag, but under WASI Preview 2 I knew I would not use it in production. Runtimes like wasmtime are single-threaded — there is no `spawn`, no background task, no way for one component to wake another when a connection is released. A [PgBouncer](https://www.pgbouncer.org/) sidecar does this job better today: it sits outside the sandbox, manages connections from multiple WASI components, and handles the concurrency the runtime cannot. I implemented the pool anyway — it is useful for native builds, for testing, and because a serious PostgreSQL library should have one. More importantly, WASI Preview 3 introduces multithreading, and when it arrives, the in-process pool will already be there, battle-tested and ready. The library will follow WASI through its iterations.

### The Bottom Line

This project exists because:

1. Infrastructure costs are not going down, and WASI is the most credible path to reducing them without sacrificing capability.
2. No existing PostgreSQL client works under WASI without massive, invasive modifications — every OS-first library hits the same wall.
3. Building WASI-first from scratch produced a cleaner, more portable architecture than retrofitting would have.
4. The WASI ecosystem needs more practical, production-grade examples — not just "hello world" demos — to prove the model works.
5. As a portfolio project, it demonstrates deep understanding of networking protocols, async I/O, cryptography, and systems programming — all under the constraints of a sandboxed runtime.

If you are reading this and thinking about building for WASI: it is harder today, but the constraints force better design. And once you ship, you can deploy anywhere.
