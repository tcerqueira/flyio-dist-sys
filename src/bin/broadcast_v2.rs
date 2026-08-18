use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_context::context::Context;

// maelstrom test -w broadcast --bin ~/.cache/cargo/target/debug/broadcast_v2 --node-count 5 --time-limit 20 --rate 10 --nemesis partition
// maelstrom test -w broadcast --bin ~/.cache/cargo/target/debug/broadcast_v2 --node-count 25 --time-limit 20 --rate 100 --latency 100
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Broadcast::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

// 3d challenge
const GOSSIP_PERIOD: Duration = Duration::from_millis(175);
// 3e challenge
// const GOSSIP_PERIOD: Duration = Duration::from_millis(275);

#[derive(Default, Clone)]
struct Broadcast {
    inner: Arc<BroadcastInner>,
}

#[derive(Default)]
struct BroadcastInner {
    // lock order: messages before neighbours
    messages: Mutex<HashSet<u64>>,
    neighbours: Mutex<HashMap<String, HashSet<u64>>>,
    gossip_handle: OnceLock<JoinHandle<()>>,
}

#[async_trait]
impl Node for Broadcast {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.broadcast(rt, req).await,
            "read" => self.read(rt, req).await,
            "topology" => self.topology(rt, req).await,
            "gossip" => self.gossip(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Broadcast {
    async fn broadcast(&self, rt: Runtime, req: Message) -> Result<()> {
        let BroadcastRequest { message, .. } = req.body.as_obj()?;
        self.messages.lock().unwrap().insert(message);

        rt.reply_ok(req).await
    }

    async fn gossip(&self, rt: Runtime, req: Message) -> Result<()> {
        let GossipRequest { messages, .. } = req.body.as_obj()?;
        let diff = {
            let empty = HashSet::new();
            let mut local = self.messages.lock().unwrap();
            let mut neighbours = self.neighbours.lock().unwrap();

            local.extend(messages.iter().copied());
            // the sender demonstrably knows what it just sent us
            if let Some(known) = neighbours.get_mut(&req.src) {
                known.extend(messages.iter().copied());
            }

            let known = neighbours.get(&req.src).unwrap_or(&empty);
            local.difference(known).copied().collect()
        };

        rt.reply(req, GossipResponse::new(diff)).await
    }

    fn neighbour_diffs(&self) -> Box<[(String, Vec<u64>)]> {
        let messages = self.messages.lock().unwrap();
        let neighbour_msgs = self.neighbours.lock().unwrap();
        neighbour_msgs
            .iter()
            .map(|(node, known)| {
                let diff = messages.difference(known).copied().collect::<Vec<_>>();
                (node.clone(), diff)
            })
            .collect()
    }

    async fn gossip_task(&self, rt: Runtime) {
        let mut interval = time::interval(GOSSIP_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;

        loop {
            interval.tick().await;

            for (node, diff) in self
                .neighbour_diffs()
                .into_iter()
                .filter(|(_, d)| !d.is_empty())
            {
                let rt = rt.clone();
                let this = Arc::clone(self);

                tokio::spawn(async move {
                    let (ctx, _handle) = Context::with_timeout(GOSSIP_PERIOD * 3);
                    let batch = diff.into_boxed_slice();

                    let exchange: Result<GossipResponse> = async {
                        let mut call = rt.rpc(&node, GossipRequest::new(batch.clone())).await?;
                        call.done_with(ctx).await?.body.as_obj()
                    }
                    .await;

                    let Ok(res) =
                        exchange.inspect_err(|e| eprintln!("gossip to {node} failed: {e}"))
                    else {
                        return;
                    };

                    this.messages
                        .lock()
                        .unwrap()
                        .extend(res.messages.iter().copied());
                    if let Some(known) = this.neighbours.lock().unwrap().get_mut(&node) {
                        known.extend(batch);
                        known.extend(res.messages);
                    }
                });
            }
        }
    }

    async fn read(&self, rt: Runtime, req: Message) -> Result<()> {
        let messages: Vec<_> = self.messages.lock().unwrap().iter().copied().collect();
        rt.reply(req, ReadResponse { messages }).await
    }

    async fn topology(&self, rt: Runtime, req: Message) -> Result<()> {
        self.gossip_handle.get_or_init(|| {
            let this = self.clone();
            let rt = rt.clone();
            tokio::spawn(async move { this.gossip_task(rt).await })
        });

        *self.neighbours.lock().unwrap() = row_column_peers(rt.node_id(), rt.nodes())
            .map(|node| (node.clone(), HashSet::new()))
            .collect();

        rt.reply_ok(req).await
    }
}

/// Row+column overlay over the node list laid out as a `cols x cols` grid: every node peers with
/// the rest of its row and the rest of its column. Degree ~2(sqrt(N) - 1), diameter 2 -- any two
/// nodes share either a row or a column with the cell at their intersection.
fn row_column_peers<'a>(node: &str, nodes: &'a [String]) -> impl Iterator<Item = &'a String> {
    let cols = (nodes.len() as f64).sqrt().ceil() as usize;
    let me = nodes.iter().position(|n| n == node);

    nodes
        .iter()
        .enumerate()
        .filter(move |(i, _)| {
            me.is_some_and(|me| *i != me && (i / cols == me / cols || i % cols == me % cols))
        })
        .map(|(_, node)| node)
}

#[derive(Serialize, Deserialize)]
struct BroadcastRequest {
    #[serde(rename = "type")]
    typ: String,
    message: u64,
}

#[derive(Serialize, Deserialize)]
struct ReadResponse {
    messages: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
struct GossipRequest {
    #[serde(rename = "type")]
    ty: String,
    messages: Box<[u64]>,
}

impl GossipRequest {
    fn new(messages: Box<[u64]>) -> Self {
        Self {
            ty: "gossip".into(),
            messages,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GossipResponse {
    #[serde(rename = "type")]
    ty: String,
    messages: Box<[u64]>,
}

impl GossipResponse {
    fn new(messages: Box<[u64]>) -> Self {
        Self {
            ty: "gossip_ok".into(),
            messages,
        }
    }
}

impl Deref for Broadcast {
    type Target = Arc<BroadcastInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
