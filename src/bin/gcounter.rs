use async_trait::async_trait;
use maelstrom::{
    Node, Result, Runtime, done,
    kv::{self, KV, Storage},
    protocol::Message,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_context::context::Context;

// maelstrom test -w g-counter --bin ~/.cache/cargo/target/debug/gcounter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
pub(crate) fn main() -> Result<()> {
    let rt = Runtime::new();
    let handler = Arc::new(Counter {
        seq_kv: RwLock::new(kv::seq_kv(rt.clone())),
    });
    Runtime::init(rt.with_handler(handler).run())
}

const TIMEOUT: Duration = Duration::from_secs(3);

struct Counter {
    seq_kv: RwLock<Storage>,
}

#[async_trait]
impl Node for Counter {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "add" => self.add(rt, req).await,
            "read" => self.read(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Counter {
    async fn add(&self, rt: Runtime, req: Message) -> Result<()> {
        let AddRequest { delta } = req.body.as_obj()?;
        {
            let kv = self.seq_kv.write().await;

            let (ctx, _handle) = Context::with_timeout(TIMEOUT);
            let curr: u64 = kv.get(ctx, rt.node_id().into()).await.unwrap_or_default(); // we assume the only error possible is the key absence

            let (ctx, _handle) = Context::with_timeout(TIMEOUT);
            kv.put(ctx, rt.node_id().into(), curr + delta).await?;
        }

        rt.reply(req, AddResponse::default()).await
    }

    async fn read(&self, rt: Runtime, req: Message) -> Result<()> {
        let (ctx, _handle) = Context::with_timeout(TIMEOUT);
        self.seq_kv
            .write()
            .await
            .put(ctx, "rand".into(), rand::random::<u64>())
            .await?;

        let mut sum = 0;
        // PERF: parallelize
        for node in rt.nodes() {
            let kv = self.seq_kv.read().await;

            let (ctx, _handle) = Context::with_timeout(TIMEOUT);
            let value: u64 = kv.get(ctx, node.clone()).await.unwrap_or_default(); // we assume the only error possible is the key absence
            sum += value;
        }

        rt.reply(req, ReadResponse::new(sum)).await
    }
}

#[derive(Serialize, Deserialize)]
struct AddRequest {
    delta: u64,
}

#[derive(Serialize, Deserialize)]
struct AddResponse {
    #[serde(rename = "type")]
    ty: String,
}

impl Default for AddResponse {
    fn default() -> Self {
        Self {
            ty: "add_ok".into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ReadResponse {
    #[serde(rename = "type")]
    ty: String,
    value: u64,
}

impl ReadResponse {
    fn new(value: u64) -> Self {
        Self {
            ty: "read_ok".into(),
            value,
        }
    }
}
