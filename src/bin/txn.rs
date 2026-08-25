use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};

// maelstrom test -w txn-rw-register --bin ~/.cache/cargo/target/debug/txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Transactions::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

type Key = u64;
type Value = u64;

#[derive(Default)]
struct Transactions {
    store: Mutex<Store>,
}

#[derive(Default)]
struct Store {
    clock: u64,
    data: Arc<HashMap<Key, (Version, Value)>>,
}

/// Derived `Ord` is lexicographic: counter first, node index to break ties.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Version(u64, usize);

#[async_trait]
impl Node for Transactions {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "txn" => self.txn(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Transactions {
    async fn txn(&self, rt: Runtime, req: Message) -> Result<()> {
        let TxnRequest { txn } = req.body.as_obj()?;
        let txn = self.execute(rt.clone(), txn);

        rt.reply(req, TxnResponse { txn }).await
    }

    fn execute(&self, rt: Runtime, txn: Vec<MicroOp>) -> Vec<MicroOp> {
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

    fn node_idx(&self, rt: Runtime) -> usize {
        static IDX: OnceLock<usize> = OnceLock::new();
        *IDX.get_or_init(|| rt.nodes().iter().position(|n| n == rt.node_id()).unwrap())
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
