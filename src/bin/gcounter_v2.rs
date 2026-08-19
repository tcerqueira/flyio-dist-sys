use async_trait::async_trait;
use maelstrom::{
    Node, Result, Runtime, done,
    kv::{self, KV, Storage},
    protocol::Message,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_context::context::Context;

// maelstrom test -w g-counter --bin ~/.cache/cargo/target/debug/gcounter_v2 --node-count 3 --rate 100 --time-limit 20 --nemesis partition
pub(crate) fn main() -> Result<()> {
    let rt = Runtime::new();
    let handler = Arc::new(Counter {
        seq_kv: kv::seq_kv(rt.clone()),
        total: Mutex::new(0),
    });
    Runtime::init(rt.with_handler(handler).run())
}

const TIMEOUT: Duration = Duration::from_secs(3);

struct Counter {
    seq_kv: Storage,
    total: Mutex<u64>,
}

#[async_trait]
impl Node for Counter {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "init" => self.init(rt, req).await,
            "add" => self.add(rt, req).await,
            "read" => self.read(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Counter {
    // the runtime emits init_ok only after this returns, so no add can observe a cold total
    async fn init(&self, rt: Runtime, req: Message) -> Result<()> {
        let (ctx, _handle) = Context::with_timeout(TIMEOUT);
        let curr: u64 = self
            .seq_kv
            .get(ctx, rt.node_id().into())
            .await
            .unwrap_or_default(); // we assume the only error possible is the key absence
        *self.total.lock().await = curr;

        done(rt, req)
    }

    async fn add(&self, rt: Runtime, req: Message) -> Result<()> {
        let AddRequest { delta } = req.body.as_obj()?;
        {
            // sole writer of this key: the lock only keeps our own puts from racing each other
            let mut total = self.total.lock().await;
            let next = *total + delta;

            let (ctx, _handle) = Context::with_timeout(TIMEOUT);
            self.seq_kv.put(ctx, rt.node_id().into(), next).await?;
            *total = next;
        }

        rt.reply(req, AddResponse::default()).await
    }

    async fn read(&self, rt: Runtime, req: Message) -> Result<()> {
        let (ctx, _handle) = Context::with_timeout(TIMEOUT);
        self.seq_kv
            .put(ctx, "rand".into(), rand::random::<u64>())
            .await?;

        let gets: Vec<_> = rt
            .nodes()
            .iter()
            .map(|node| {
                let (kv, node) = (self.seq_kv.clone(), node.clone());
                rt.spawn(async move {
                    let (ctx, _handle) = Context::with_timeout(TIMEOUT);
                    kv.get::<u64>(ctx, node).await.unwrap_or_default() // ditto
                })
            })
            .collect();

        let mut sum = 0;
        for get in gets {
            sum += get.await?;
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
