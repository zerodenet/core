use super::{
    executor::{Inbox, Work},
    failure::{self, Failure},
    Options,
};
use crate::split_http::xmux::prepared::{self, PreparedH2};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::oneshot;
use zero_platform_tokio::{
    contract::{ChannelError, Reason, Stage},
    controlled_carrier::CarrierIo,
};

type Driver = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>;
struct Life {
    inbox: Arc<Inbox>,
    completed: bool,
}
impl Drop for Life {
    fn drop(&mut self) {
        self.inbox.close();
        if !self.completed {
            self.inbox.failure.set(self.inbox.failure.local(
                Stage::Close,
                Reason::Revoked,
                std::io::Error::other("H2 aggregate driver exited"),
            ));
        }
        // Work guards return occupancy tokens under the lock: never drop queued
        // originals while holding that same lock. The driver owns the sole inbox.
        while let Some(work) = self.inbox.pop() {
            drop(work);
        }
    }
}

pub(super) async fn run<B>(
    io: CarrierIo,
    options: Options,
    failure: Arc<Failure>,
    handoff: oneshot::Sender<hyper::client::conn::http2::SendRequest<B>>,
) -> Result<(), ChannelError>
where
    B: hyper::body::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let inbox = Inbox::new(options.delegate_limit, failure.clone());
    let mut life = Life {
        inbox: inbox.clone(),
        completed: false,
    };
    let mut handshake = Some(Box::pin(prepared::http2_with_body::<_, B, _>(
        io,
        options.settings.as_deref(),
        options.keepalive,
        inbox.executor(),
    )));
    let mut handoff = Some(handoff);
    let mut outer: Option<Driver> = None;
    let mut active = FuturesUnordered::<Work>::new();
    loop {
        let changed = inbox.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        while let Some(work) = inbox.pop() {
            active.push(work);
        }
        if let Some(source) = failure.get() {
            return Err(failure::forwarded(source));
        }
        if handshake.is_none() && outer.is_none() && active.is_empty() {
            life.completed = true;
            return Ok(());
        }
        tokio::select! {biased;
            _ = options.cancel.cancelled() => {
                let source=failure.set(failure.local(Stage::Close,Reason::Cancelled,std::io::Error::new(std::io::ErrorKind::ConnectionAborted,"H2 aggregate lifetime cancelled")));
                return Err(failure::forwarded(source));
            }
            source=failure.wait()=>return Err(failure::forwarded(source)),
            result=async { handshake.as_mut().expect("guarded handshake").as_mut().await }, if handshake.is_some()=>{
                handshake=None;
                match result {
                    Ok(PreparedH2{sender,driver})=>{
                        outer=Some(driver);
                        // A dropped ready receiver must not leave a healthy
                        // connection with no consumer. No pool is manufactured.
                        if handoff.take().expect("single handshake handoff").send(sender).is_err() {
                            let source=failure.set(failure.local(Stage::Handshake,Reason::Cancelled,std::io::Error::other("H2 handshake consumer dropped")));
                            return Err(failure::forwarded(source));
                        }
                    }
                    Err(source)=>{
                        let source=failure.set(failure.local(Stage::Handshake,Reason::Io,source));
                        return Err(failure::forwarded(source));
                    }
                }
            }
            result=async {outer.as_mut().expect("guarded outer driver").as_mut().await},if outer.is_some()=>{
                outer=None;
                if let Err(source)=result {
                    let source=failure.set(failure.local(Stage::Receive,Reason::Io,source));
                    return Err(failure::forwarded(source));
                }
                // Outer Ready alone is not aggregate completion. All queued
                // and active original delegates must also leave this task.
                inbox.close();
            }
            _=active.next(),if !active.is_empty()=>{},
            _=changed=>{},
        }
    }
}
