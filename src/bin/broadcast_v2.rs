use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// maelstrom test -w broadcast --bin ~/.cache/cargo/target/debug/broadcast_v2 --node-count 5 --time-limit 20 --rate 10 --nemesis partition
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Broadcast::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

#[derive(Default)]
struct Broadcast {
    messages: Mutex<HashMap<String, HashSet<u64>>>,
}

#[async_trait]
impl Node for Broadcast {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.broadcast(rt, req).await,
            "read" => self.read(rt, req).await,
            "topology" => self.topology(rt, req).await,
            "broadcast_ok" => Ok(()),
            _ => done(rt, req),
        }
    }
}

impl Broadcast {
    async fn broadcast(&self, rt: Runtime, req: Message) -> Result<()> {
        let BroadcastRequest { message, .. } = req.body.as_obj()?;

        for (node, diff) in self.gossip_plan(rt.node_id(), &req.src, message) {
            for message in diff {
                rt.send_async(node.as_str(), BroadcastRequest::new(message))?;
            }
        }

        rt.reply_ok(req).await
    }

    fn gossip_plan(&self, node_id: &str, src: &str, message: u64) -> Box<[(String, Vec<u64>)]> {
        let mut messages = self.messages.lock().unwrap();
        // register the sender knows about `message`
        if let Some(sender) = messages.get_mut(src) {
            sender.insert(message);
        }
        // termination condition, if we already know about the `message` dont broadcast it
        let inserted = messages.entry(node_id.into()).or_default().insert(message);
        if !inserted {
            return Box::new([]);
        }

        let local_msgs = messages.get(node_id).expect("inserted above");
        messages
            .iter()
            .filter(|(node, _)| node.as_str() != node_id && node.as_str() != src)
            .map(|(node, known)| {
                let diff = local_msgs.difference(known).copied().collect::<Vec<_>>();
                (node.clone(), diff)
            })
            .collect()
    }

    async fn read(&self, rt: Runtime, req: Message) -> Result<()> {
        let messages: Vec<_> = self
            .messages
            .lock()
            .unwrap()
            .entry(rt.node_id().into())
            .or_default()
            .iter()
            .copied()
            .collect();
        rt.reply(req, ReadResponse { messages }).await
    }

    async fn topology(&self, rt: Runtime, req: Message) -> Result<()> {
        let TopologyRequest {
            topology: mut topology_req,
        } = req.body.as_obj()?;

        let neighbors = topology_req
            .remove(rt.node_id())
            .expect("node id present in topology");

        {
            let mut messages = self.messages.lock().unwrap();
            let local_msgs = messages.remove(rt.node_id()).unwrap_or_default();
            *messages = neighbors
                .into_iter()
                .map(|node| (node, HashSet::new()))
                .chain([(rt.node_id().to_string(), local_msgs)])
                .collect();
        }

        rt.reply_ok(req).await
    }
}

#[derive(Serialize, Deserialize)]
struct BroadcastRequest {
    #[serde(rename = "type")]
    typ: String,
    message: u64,
}

impl BroadcastRequest {
    fn new(message: u64) -> Self {
        Self {
            typ: "broadcast".into(),
            message,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TopologyRequest {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct ReadResponse {
    messages: Vec<u64>,
}
