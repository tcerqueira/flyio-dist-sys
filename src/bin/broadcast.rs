use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

// maelstrom test -w broadcast --bin ~/.cache/cargo/target/debug/broadcast --node-count 5 --time-limit 20 --rate 10
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Broadcast::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

#[derive(Default)]
struct Broadcast {
    messages: Mutex<HashSet<u64>>,
    neighbours: RwLock<Vec<String>>,
}

#[async_trait]
impl Node for Broadcast {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.broadcast(rt, req).await,
            "read" => self.read(rt, req).await,
            "topology" => self.topology(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Broadcast {
    async fn broadcast(&self, rt: Runtime, req: Message) -> Result<()> {
        let BroadcastRequest { message } = req.body.as_obj()?;
        let inserted = self.messages.lock().unwrap().insert(message);
        if !inserted {
            return rt.reply_ok(req).await;
        }

        let nodes = self
            .neighbours
            .read()
            .unwrap()
            .iter()
            .filter(|n| **n != req.src)
            .cloned()
            .collect::<Box<[_]>>();

        for node in nodes {
            rt.send_async(node, req.body.clone())?;
        }

        rt.reply_ok(req).await
    }

    async fn read(&self, rt: Runtime, req: Message) -> Result<()> {
        let messages: Vec<_> = self.messages.lock().unwrap().iter().copied().collect();
        rt.reply(req, ReadResponse { messages }).await
    }

    async fn topology(&self, rt: Runtime, req: Message) -> Result<()> {
        let TopologyRequest {
            topology: mut topology_req,
        } = req.body.as_obj()?;

        let neighbours = topology_req
            .remove(rt.node_id())
            .expect("node id present in topology");
        *self.neighbours.write().unwrap() = neighbours;

        rt.reply_ok(req).await
    }
}

#[derive(Serialize, Deserialize)]
struct BroadcastRequest {
    message: u64,
}

#[derive(Serialize, Deserialize)]
struct TopologyRequest {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct ReadResponse {
    messages: Vec<u64>,
}
