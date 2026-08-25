use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_context::context::Context;

// Every key is owned by exactly one node, so offsets are assigned under a local
// lock and never coordinated. Trades partition tolerance for latency: a key is
// unreachable while its owner is, and log state dies with the node.
//
// maelstrom test -w kafka --bin ~/.cache/cargo/target/debug/kafka_v3 --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Kafka::default());
    Runtime::init(Runtime::new().with_handler(handler).run())
}

const TIMEOUT: Duration = Duration::from_secs(3);

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
        let SendRequest { key, msg, .. } = req.body.as_obj()?;
        let owner = key_owner(rt.nodes(), &key).to_string();

        let offset = if owner == rt.node_id() {
            self.append(key, msg)
        } else {
            let (ctx, _handle) = Context::with_timeout(TIMEOUT);
            let res = rt.call(ctx, owner, SendRequest::new(key, msg)).await?;
            res.body.as_obj::<SendResponse>()?.offset
        };

        rt.reply(req, SendResponse::new(offset)).await
    }

    async fn poll(&self, rt: Runtime, req: Message) -> Result<()> {
        let PollRequest { offsets, .. } = req.body.as_obj()?;
        let mut by_owner = shard_entries(&rt, offsets);

        let mut msgs = self.read(&by_owner.remove(rt.node_id()).unwrap_or_default());
        let calls = fan_out(
            &rt,
            by_owner
                .into_iter()
                .map(|(node, offsets)| (node, PollRequest::new(offsets))),
        );

        for call in calls {
            let PollResponse { msgs: remote, .. } = call.await??.body.as_obj()?;
            msgs.extend(remote);
        }

        rt.reply(req, PollResponse::new(msgs)).await
    }

    async fn commit_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let CommitRequest { offsets, .. } = req.body.as_obj()?;
        let mut by_owner = shard_entries(&rt, offsets);

        self.commit(&by_owner.remove(rt.node_id()).unwrap_or_default());
        let calls = fan_out(
            &rt,
            by_owner
                .into_iter()
                .map(|(node, offsets)| (node, CommitRequest::new(offsets))),
        );

        for call in calls {
            call.await??;
        }

        rt.reply_ok(req).await
    }

    async fn list_committed_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let ListCommitRequest { keys, .. } = req.body.as_obj()?;
        let mut by_owner = shard_keys(&rt, keys);

        let mut offsets = self.committed(&by_owner.remove(rt.node_id()).unwrap_or_default());
        let calls = fan_out(
            &rt,
            by_owner
                .into_iter()
                .map(|(node, keys)| (node, ListCommitRequest::new(keys))),
        );

        for call in calls {
            let ListCommitResponse {
                offsets: remote, ..
            } = call.await??.body.as_obj()?;
            offsets.extend(remote);
        }

        rt.reply(req, ListCommitResponse::new(offsets)).await
    }

    fn append(&self, key: Box<str>, msg: u64) -> usize {
        let mut logs = self.logs.lock().unwrap();
        let log = logs.entry(key).or_default();
        log.push(msg);
        log.len() - 1
    }

    fn read(&self, offsets: &HashMap<Box<str>, usize>) -> HashMap<Box<str>, Vec<(usize, u64)>> {
        let logs = self.logs.lock().unwrap();
        offsets
            .iter()
            .filter_map(|(key, &start)| {
                let entries: Vec<_> = logs
                    .get(key)?
                    .iter()
                    .enumerate()
                    .skip(start)
                    .map(|(offset, &msg)| (offset, msg))
                    .collect();
                (!entries.is_empty()).then(|| (key.clone(), entries))
            })
            .collect()
    }

    fn commit(&self, offsets: &HashMap<Box<str>, usize>) {
        let mut commits = self.commits.lock().unwrap();
        for (key, &offset) in offsets {
            let current = commits.entry(key.clone()).or_insert(offset);
            *current = (*current).max(offset);
        }
    }

    fn committed(&self, keys: &[Box<str>]) -> HashMap<Box<str>, usize> {
        let commits = self.commits.lock().unwrap();
        keys.iter()
            .filter_map(|key| Some((key.clone(), *commits.get(key)?)))
            .collect()
    }
}

fn key_owner<'a>(nodes: &'a [String], key: &str) -> &'a str {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    &nodes[hasher.finish() as usize % nodes.len()]
}

fn shard_entries<V>(
    rt: &Runtime,
    entries: HashMap<Box<str>, V>,
) -> HashMap<String, HashMap<Box<str>, V>> {
    entries
        .into_iter()
        .fold(HashMap::new(), |mut acc, (key, value)| {
            let owner = key_owner(rt.nodes(), &key).to_string();
            acc.entry(owner).or_default().insert(key, value);
            acc
        })
}

fn shard_keys(rt: &Runtime, keys: Box<[Box<str>]>) -> HashMap<String, Vec<Box<str>>> {
    keys.into_vec()
        .into_iter()
        .fold(HashMap::new(), |mut acc, key| {
            let owner = key_owner(rt.nodes(), &key).to_string();
            acc.entry(owner).or_default().push(key);
            acc
        })
}

fn fan_out<R>(
    rt: &Runtime,
    requests: impl IntoIterator<Item = (String, R)>,
) -> Vec<JoinHandle<Result<Message>>>
where
    R: Serialize + Send + 'static,
{
    requests
        .into_iter()
        .map(|(node, request)| {
            let caller = rt.clone();
            rt.spawn(async move {
                let (ctx, _handle) = Context::with_timeout(TIMEOUT);
                caller.call(ctx, node, request).await
            })
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
struct SendRequest {
    #[serde(rename = "type")]
    ty: Box<str>,
    key: Box<str>,
    msg: u64,
}

impl SendRequest {
    fn new(key: Box<str>, msg: u64) -> Self {
        Self {
            ty: "send".into(),
            key,
            msg,
        }
    }
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
    #[serde(rename = "type")]
    ty: Box<str>,
    offsets: HashMap<Box<str>, usize>,
}

impl PollRequest {
    fn new(offsets: HashMap<Box<str>, usize>) -> Self {
        Self {
            ty: "poll".into(),
            offsets,
        }
    }
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
    #[serde(rename = "type")]
    ty: Box<str>,
    offsets: HashMap<Box<str>, usize>,
}

impl CommitRequest {
    fn new(offsets: HashMap<Box<str>, usize>) -> Self {
        Self {
            ty: "commit_offsets".into(),
            offsets,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ListCommitRequest {
    #[serde(rename = "type")]
    ty: Box<str>,
    keys: Box<[Box<str>]>,
}

impl ListCommitRequest {
    fn new(keys: Vec<Box<str>>) -> Self {
        Self {
            ty: "list_committed_offsets".into(),
            keys: keys.into_boxed_slice(),
        }
    }
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
