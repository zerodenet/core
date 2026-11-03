//! One framed request/response exchange. Policy and upstream selection stay
//! outside this role; the protocol owns only message framing and continuation.
use super::{BoxFuture, ChannelError, StreamActivity};
use crate::{rate_limit::SharedRateLimiter, TcpRelayStream};
use std::{future::Future, sync::Arc};
use zero_core::request::{Destination, RequestFacts};
use zero_traits::IoObserver;

pub enum MessageAction {
    Transfer {
        upstream: TcpRelayStream,
        effective_target: Destination,
        context: MessageContext,
    },
    Blocked,
    UpstreamFailed,
    Redirect {
        status: u16,
        location: String,
    },
}

/// An upgraded message transfers its actual two streams back to the common
/// duplex executor; the protocol does not run routing or a second relay engine.
pub enum MessageTransfer {
    Complete,
    Upgraded {
        inbound: TcpRelayStream,
        upstream: TcpRelayStream,
    },
}

pub trait MessageReply: Send + 'static {
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self,
        action: MessageAction,
    ) -> impl Future<Output = Result<MessageTransfer, ChannelError>> + Send;
}

pub trait AcceptedMessage: Send {
    fn facts(&self) -> &RequestFacts;
    fn respond(
        self: Box<Self>,
        action: MessageAction,
    ) -> BoxFuture<'static, Result<MessageTransfer, ChannelError>>;
}
struct Message<T>(T);
impl<T: MessageReply> AcceptedMessage for Message<T> {
    fn facts(&self) -> &RequestFacts {
        self.0.facts()
    }
    fn respond(
        self: Box<Self>,
        action: MessageAction,
    ) -> BoxFuture<'static, Result<MessageTransfer, ChannelError>> {
        Box::pin(self.0.respond(action))
    }
}
pub fn accept_message(reply: impl MessageReply) -> Box<dyn AcceptedMessage> {
    Box::new(Message(reply))
}

/// Short-lived execution facts for one message. These are the actual shared
/// rate timelines and boundary observers, with no engine/configuration access.
#[derive(Clone)]
pub struct MessageContext {
    upload: Option<SharedRateLimiter>,
    download: Option<SharedRateLimiter>,
    activity: StreamActivity,
    inbound: Arc<dyn IoObserver>,
    outbound: Arc<dyn IoObserver>,
}
impl MessageContext {
    pub fn new(
        upload: Option<SharedRateLimiter>,
        download: Option<SharedRateLimiter>,
        activity: StreamActivity,
        inbound: Arc<dyn IoObserver>,
        outbound: Arc<dyn IoObserver>,
    ) -> Self {
        Self {
            upload,
            download,
            activity,
            inbound,
            outbound,
        }
    }
    pub fn upload_limiter(&self) -> Option<SharedRateLimiter> {
        self.upload.clone()
    }
    pub fn download_limiter(&self) -> Option<SharedRateLimiter> {
        self.download.clone()
    }
    pub fn record_activity(&self) {
        self.activity.touch();
    }
    pub fn record_upload(&self, bytes: u64) {
        self.record_upload_io(bytes, bytes);
    }
    pub fn record_download(&self, bytes: u64) {
        self.record_download_io(bytes, bytes);
    }
    pub fn record_upload_io(&self, inbound_rx: u64, outbound_tx: u64) {
        if inbound_rx != 0 || outbound_tx != 0 {
            self.record_activity();
        }
        observe(inbound_rx, |n| self.inbound.received(n));
        observe(outbound_tx, |n| self.outbound.sent(n));
    }
    pub fn record_download_io(&self, outbound_rx: u64, inbound_tx: u64) {
        if outbound_rx != 0 || inbound_tx != 0 {
            self.record_activity();
        }
        observe(outbound_rx, |n| self.outbound.received(n));
        observe(inbound_tx, |n| self.inbound.sent(n));
    }
}
fn observe(mut bytes: u64, mut record: impl FnMut(usize)) {
    while bytes != 0 {
        let chunk = bytes.min(usize::MAX as u64) as usize;
        record(chunk);
        bytes -= chunk as u64;
    }
}
