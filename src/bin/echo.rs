use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use std::sync::Arc;

// maelstrom test -w echo --bin ~/.cache/cargo/target/debug/echo --node-count 1 --time-limit 10
pub(crate) fn main() -> Result<()> {
    let handler = Arc::new(Echo);
    Runtime::init(Runtime::new().with_handler(handler).run())
}

#[derive(Default)]
struct Echo;

#[async_trait]
impl Node for Echo {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        if req.get_type() != "echo" {
            return done(rt, req);
        }
        let echo = req.body.clone().with_type("echo_ok");
        rt.reply(req, echo).await
    }
}
