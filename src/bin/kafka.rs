use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};

// maelstrom test -w kafka --bin ~/.cache/cargo/target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Kafka::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

#[derive(Default)]
struct Kafka {
    logs: Mutex<HashMap<Box<str>, Vec<u64>>>,
    commits: Mutex<HashMap<Box<str>, usize>>,
}

#[async_trait]
impl Node for Kafka {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "send" => self.send(rt, req).await,
            "poll" => self.poll(rt, req).await,
            "commit_offsets" => self.commit_offsets(rt, req).await,
            "list_committed_offsets" => self.list_committed_offsets(rt, req).await,
            _ => done(rt, req),
        }
    }
}

impl Kafka {
    async fn send(&self, rt: Runtime, req: Message) -> Result<()> {
        let SendRequest { key, msg } = req.body.as_obj()?;
        let offset = self
            .logs
            .lock()
            .unwrap()
            .entry(key)
            .and_modify(|l| l.push(msg))
            .or_insert_with(|| vec![msg])
            .len()
            - 1;

        rt.reply(req, SendResponse::new(offset)).await
    }

    async fn poll(&self, rt: Runtime, req: Message) -> Result<()> {
        let PollRequest { offsets } = req.body.as_obj()?;

        let msgs = {
            let logs = self.logs.lock().unwrap();
            offsets
                .into_iter()
                .flat_map(|(key, offset)| {
                    let msgs_slice = logs.get(&key).and_then(|v| v.get(offset..));
                    msgs_slice.map(|l| {
                        (
                            key,
                            l.iter()
                                .enumerate()
                                .map(|(i, m)| (i + offset, *m))
                                .collect(),
                        )
                    })
                })
                .collect()
        };

        rt.reply(req, PollResponse::new(msgs)).await
    }

    async fn commit_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let CommitRequest { offsets } = req.body.as_obj()?;
        {
            let mut commits = self.commits.lock().unwrap();
            for (k, mut offset) in offsets {
                commits
                    .entry(k)
                    .and_modify(|v| *v = *v.max(&mut offset))
                    .or_insert(offset);
            }
        }
        rt.reply_ok(req).await
    }

    async fn list_committed_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let ListCommitRequest { keys } = req.body.as_obj()?;
        let offsets = {
            let commits = self.commits.lock().unwrap();
            keys.into_iter()
                .flat_map(|k| commits.get(&k).map(|c| (k, *c)))
                .collect()
        };

        rt.reply(req, ListCommitResponse::new(offsets)).await
    }
}

#[derive(Serialize, Deserialize)]
struct SendRequest {
    key: Box<str>,
    msg: u64,
}

#[derive(Serialize, Deserialize)]
struct SendResponse {
    #[serde(rename = "type")]
    ty: Box<str>,
    offset: usize,
}

impl SendResponse {
    fn new(offset: usize) -> Self {
        Self {
            ty: "send_ok".into(),
            offset,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PollRequest {
    offsets: HashMap<Box<str>, usize>,
}

#[derive(Serialize, Deserialize)]
struct PollResponse {
    #[serde(rename = "type")]
    ty: Box<str>,
    msgs: HashMap<Box<str>, Vec<(usize, u64)>>,
}

impl PollResponse {
    fn new(msgs: HashMap<Box<str>, Vec<(usize, u64)>>) -> Self {
        Self {
            ty: "poll_ok".into(),
            msgs,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct CommitRequest {
    offsets: HashMap<Box<str>, usize>,
}

#[derive(Serialize, Deserialize)]
struct ListCommitRequest {
    keys: Box<[Box<str>]>,
}

#[derive(Serialize, Deserialize)]
struct ListCommitResponse {
    #[serde(rename = "type")]
    ty: Box<str>,
    offsets: HashMap<Box<str>, usize>,
}

impl ListCommitResponse {
    fn new(offsets: HashMap<Box<str>, usize>) -> Self {
        Self {
            ty: "list_committed_offsets_ok".into(),
            offsets,
        }
    }
}
