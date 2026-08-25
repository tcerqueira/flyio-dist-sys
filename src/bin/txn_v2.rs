#![feature(btree_merge)]

use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};

// maelstrom test -w txn-rw-register --bin ~/.cache/cargo/target/debug/txn_v2 --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
// maelstrom test -w txn-rw-register --bin ~/.cache/cargo/target/debug/txn_v2 --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Transactions::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

const GOSSIP_PERIOD: Duration = Duration::from_millis(200);

type Key = u64;
type Value = u64;
type Data = BTreeMap<Key, (Version, Value)>;

#[derive(Default, Clone)]
struct Transactions {
    inner: Arc<Inner>,
}

impl Deref for Transactions {
    type Target = Arc<Inner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Default)]
struct Inner {
    store: Mutex<Store>,
    idx: OnceLock<usize>,
}

#[derive(Default)]
struct Store {
    clock: u64,
    data: Arc<Data>,
}

/// Derived `Ord` is lexicographic: counter first, node index to break ties.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Version(u64, usize);

#[async_trait]
impl Node for Transactions {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "init" => self.init(rt, req).await,
            "txn" => self.txn(rt, req).await,
            "gossip" => self.gossip(req),
            _ => done(rt, req),
        }
    }
}

impl Transactions {
    async fn init(&self, rt: Runtime, req: Message) -> Result<()> {
        self.node_idx(&rt);

        let this = self.clone();
        let gossip_rt = rt.clone();
        tokio::spawn(async move { this.gossip_task(gossip_rt).await });

        done(rt, req)
    }

    async fn txn(&self, rt: Runtime, req: Message) -> Result<()> {
        let TxnRequest { txn } = req.body.as_obj()?;
        let txn = self.execute(&rt, txn);

        rt.reply(req, TxnResponse { txn }).await
    }

    fn execute(&self, rt: &Runtime, txn: Vec<MicroOp>) -> Vec<MicroOp> {
        let node_idx = self.node_idx(rt);
        let mut store = self.store.lock().unwrap();
        let clock = store.clock + 1;
        store.clock = clock;

        let data = Arc::make_mut(&mut store.data);
        txn.into_iter()
            .map(|op| match op.0 {
                Op::Read => {
                    let value = data.get(&op.1).map(|(_v, d)| d).copied();
                    (Op::Read, op.1, value)
                }
                Op::Write => {
                    data.insert(op.1, (Version(clock, node_idx), op.2.unwrap()));
                    op
                }
            })
            .collect()
    }

    fn gossip(&self, req: Message) -> Result<()> {
        let GossipIn { state } = req.body.as_obj()?;
        self.merge(state);
        Ok(())
    }

    async fn gossip_task(&self, rt: Runtime) {
        let mut interval = time::interval(GOSSIP_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;

        loop {
            interval.tick().await;

            let snapshot = self.snapshot();
            for node in rt.nodes().iter().filter(|n| *n != rt.node_id()) {
                rt.call_async(node, GossipOut::new(snapshot.clone()));
            }
        }
    }

    fn merge(&self, peer_data: Data) {
        let mut store = self.store.lock().unwrap();

        store.clock = store.clock.max(
            peer_data
                .values()
                .map(|(Version(counter, _), _)| *counter)
                .max()
                .unwrap_or(0),
        );

        let data = Arc::make_mut(&mut store.data);
        data.merge(peer_data, |_, local, peer| match local.cmp(&peer) {
            std::cmp::Ordering::Less => peer,
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => local,
        });
    }

    fn snapshot(&self) -> Arc<Data> {
        Arc::clone(&self.store.lock().unwrap().data)
    }

    fn node_idx(&self, rt: &Runtime) -> usize {
        *self
            .idx
            .get_or_init(|| rt.nodes().iter().position(|n| n == rt.node_id()).unwrap())
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
}

type MicroOp = (Op, Key, Option<Value>);

#[derive(Deserialize)]
struct TxnRequest {
    txn: Vec<MicroOp>,
}

#[derive(Serialize)]
struct TxnResponse {
    txn: Vec<MicroOp>,
}

#[derive(Serialize)]
struct GossipOut {
    #[serde(rename = "type")]
    ty: &'static str,
    state: Arc<Data>,
}

impl GossipOut {
    fn new(state: Arc<Data>) -> Self {
        Self {
            ty: "gossip",
            state,
        }
    }
}

#[derive(Deserialize)]
struct GossipIn {
    state: Data,
}
