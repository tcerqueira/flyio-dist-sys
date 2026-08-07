use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use rand::random;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// maelstrom test -w unique-ids --bin ~/.cache/cargo/target/debug/unique --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Unique { id: random() });
    Runtime::init(Runtime::new().with_handler(handler).run())
}

struct Unique {
    id: u64,
}

#[async_trait]
impl Node for Unique {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        if req.get_type() != "generate" {
            return done(rt, req);
        }
        let id = format!("{}-{}", self.id, random::<u64>());
        rt.reply(req, Payload { id }).await
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {
    id: String,
}
