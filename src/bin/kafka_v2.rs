use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use maelstrom::kv::{self, KV, Storage};
use maelstrom::protocol::Message;
use maelstrom::{Error, Node, Result, Runtime, done};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_context::context::Context;

// maelstrom test -w kafka --bin ~/.cache/cargo/target/debug/kafka_v2 --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
pub(crate) fn main() -> Result<()> {
    let rt = Runtime::new();
    let handler = Arc::new(Kafka {
        kv: kv::lin_kv(rt.clone()),
    });
    Runtime::init(rt.with_handler(handler).run())
}

const TIMEOUT: Duration = Duration::from_secs(3);

struct Kafka {
    kv: Storage,
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
        let offset = append(&self.kv, &key, msg).await?;

        rt.reply(req, SendResponse::new(offset)).await
    }

    async fn poll(&self, rt: Runtime, req: Message) -> Result<()> {
        let PollRequest { offsets } = req.body.as_obj()?;

        let reads: Vec<_> = offsets
            .into_iter()
            .map(|(key, start)| {
                let kv = self.kv.clone();
                rt.spawn(async move {
                    read::<Vec<u64>>(&kv, log_key(&key))
                        .await
                        .map(|log| (key, entries_from(log.unwrap_or_default(), start)))
                })
            })
            .collect();

        let mut msgs = HashMap::new();
        for read in reads {
            let (key, entries) = read.await??;
            if !entries.is_empty() {
                msgs.insert(key, entries);
            }
        }

        rt.reply(req, PollResponse::new(msgs)).await
    }

    async fn commit_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let CommitRequest { offsets } = req.body.as_obj()?;

        let writes: Vec<_> = offsets
            .into_iter()
            .map(|(key, offset)| {
                let kv = self.kv.clone();
                rt.spawn(async move { advance_commit(&kv, &key, offset).await })
            })
            .collect();

        for write in writes {
            write.await??;
        }

        rt.reply_ok(req).await
    }

    async fn list_committed_offsets(&self, rt: Runtime, req: Message) -> Result<()> {
        let ListCommitRequest { keys } = req.body.as_obj()?;

        let reads: Vec<_> = keys
            .into_vec()
            .into_iter()
            .map(|key| {
                let kv = self.kv.clone();
                rt.spawn(async move {
                    read(&kv, commit_key(&key))
                        .await
                        .map(|offset| offset.map(|o| (key, o)))
                })
            })
            .collect();

        let mut offsets = HashMap::new();
        for read in reads {
            // a key with no commit is omitted, not reported as 0
            if let Some((key, offset)) = read.await?? {
                offsets.insert(key, offset);
            }
        }

        rt.reply(req, ListCommitResponse::new(offsets)).await
    }
}

fn log_key(key: &str) -> String {
    format!("log:{key}")
}

fn commit_key(key: &str) -> String {
    format!("cmt:{key}")
}

fn is_code(err: &(dyn std::error::Error + Send + Sync + 'static), code: &Error) -> bool {
    err.downcast_ref::<Error>() == Some(code)
}

async fn read<T: DeserializeOwned + Send>(kv: &Storage, key: String) -> Result<Option<T>> {
    let (ctx, _handle) = Context::with_timeout(TIMEOUT);
    match kv.get::<T>(ctx, key).await {
        Ok(value) => Ok(Some(value)),
        Err(e) if is_code(e.as_ref(), &Error::KeyDoesNotExist) => Ok(None),
        Err(e) => Err(e),
    }
}

/// One CAS assigns the offset and stores the message, so a message exists
/// exactly when its offset does — the log can never contain a hole.
async fn append(kv: &Storage, key: &str, msg: u64) -> Result<usize> {
    loop {
        let log: Vec<u64> = read(kv, log_key(key)).await?.unwrap_or_default();
        let appended: Vec<u64> = log.iter().copied().chain([msg]).collect();
        let offset = log.len();

        let (ctx, _handle) = Context::with_timeout(TIMEOUT);
        match kv.cas(ctx, log_key(key), log, appended, true).await {
            Ok(()) => return Ok(offset),
            Err(e) if is_code(e.as_ref(), &Error::PreconditionFailed) => continue,
            Err(e) => return Err(e),
        }
    }
}

fn entries_from(log: Vec<u64>, start: usize) -> Vec<(usize, u64)> {
    log.into_iter().enumerate().skip(start).collect()
}

/// Committed offsets only ever move forward. A lost update here would rewind a
/// consumer and re-deliver messages, which is permitted; letting one go
/// backwards past a message that was never delivered would not be.
async fn advance_commit(kv: &Storage, key: &str, offset: usize) -> Result<()> {
    loop {
        let current = read::<usize>(kv, commit_key(key)).await?.unwrap_or(0);
        if offset <= current {
            return Ok(());
        }

        let (ctx, _handle) = Context::with_timeout(TIMEOUT);
        match kv.cas(ctx, commit_key(key), current, offset, true).await {
            Ok(()) => return Ok(()),
            Err(e) if is_code(e.as_ref(), &Error::PreconditionFailed) => continue,
            Err(e) => return Err(e),
        }
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
