# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Rust solutions to the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/), tested against
[Jepsen Maelstrom](https://github.com/jepsen-io/maelstrom). One binary per challenge.

## Environment

The dev shell is a Nix flake (`flake.nix`) wired up via direnv (`.envrc` contains `use flake`). It provides the Rust
toolchain (driven by `rust-toolchain.toml`) and `maelstrom-clj`, which puts the `maelstrom` CLI on `PATH` bundled with
its own JDK, gnuplot and graphviz. If `maelstrom` is missing, the shell isn't active — run `nix develop` or `direnv allow`.

**`CARGO_TARGET_DIR` is set globally in the user's environment to `~/.cache/cargo/target`**, so built binaries land
there, not in `./target`. Every `maelstrom test --bin` path must point at `~/.cache/cargo/target/debug/<name>`.

## Commands

```sh
cargo build                 # builds every src/bin/*.rs
cargo clippy                # use instead of cargo check
cargo fmt

# Run a challenge under Maelstrom. Each binary carries its exact invocation
# as a comment above main() — copy it from there rather than reconstructing it.
maelstrom test -w echo       --bin ~/.cache/cargo/target/debug/echo   --node-count 1 --time-limit 10
maelstrom test -w unique-ids --bin ~/.cache/cargo/target/debug/unique --time-limit 30 --rate 1000 \
    --node-count 3 --availability total --nemesis partition

maelstrom serve             # browse results from ./store (gitignored) at localhost:8080
```

There are no `cargo test` unit tests; Maelstrom runs *are* the test suite. Always `cargo build` before a Maelstrom run
— the CLI executes a stale binary silently otherwise.

## Architecture

`src/bin/<challenge>.rs` — one binary per challenge, auto-discovered by Cargo (no `[[bin]]` entries). `src/lib.rs` is
currently empty; it's where shared code between challenges belongs (crate name `flyio_dist_sys`).

The `maelstrom-node` dependency is imported as `maelstrom::` — the package and lib names differ, which trips up
searching.

Every binary follows the same shape:

```rust
pub(crate) fn main() -> Result<()> {
    Runtime::init(Runtime::new().with_handler(Arc::new(Handler)).run())
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        if req.get_type() != "<workload_msg>" {
            return done(rt, req);   // init/topology/etc. handled by the runtime
        }
        rt.reply(req, payload).await
    }
}
```

`Runtime` owns the stdin/stdout JSON-lines protocol, so handlers only implement workload logic. Key points:

- `done(rt, req)` is the catch-all for message types a handler doesn't own — never drop a message silently.
- **stdout is the protocol.** Never `println!` for debugging; use `maelstrom::log` (stderr).
- Handlers are `&self` behind an `Arc` and run concurrently, so per-node mutable state needs interior mutability
  (`Mutex`/`RwLock`/atomics) inside the handler struct.
- `rt.reply` sets `in_reply_to` and, unless the payload already serializes a `type` field, fills in
  `"<request_type>_ok"` for you. `rt.reply_ok(req)` sends the empty `_ok` body.
- Reply payloads are plain `#[derive(Serialize, Deserialize)]` structs (see `Payload` in `src/bin/unique.rs`), or
  `req.body.clone().with_type("..._ok")` to echo a body back.

Useful `Runtime` APIs for the later challenges: `node_id()`, `nodes()`, `neighbours()`, `send`/`send_async` for
fire-and-forget gossip, `rpc`/`call`/`call_async` (with `tokio_context::Context` for timeouts) for request/response
between nodes, `spawn` for background tasks, and `maelstrom::kv::{lin_kv, seq_kv, lww_kv, tso_kv}` for the
Maelstrom-provided key-value services used by the counter and transaction challenges.

## Conventions

- Keep the exact `maelstrom test` command for a challenge as a comment directly above `main()` in its binary.
- Commits follow Conventional Commits, scoped to the challenge (`feat(unique): ...`).
