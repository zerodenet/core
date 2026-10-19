use crate::contract_support as support;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::Notify,
    time::{timeout, Instant},
};
use tokio_util::sync::CancellationToken;
use zero_core::{component::SessionId, request::RequestFacts};
use zero_platform_tokio::contract::{
    context::{AcceptContext, HandshakeContext},
    inbound::*,
    ChannelError,
};

struct Reply {
    facts: RequestFacts,
    io: DuplexStream,
    replies: Arc<AtomicUsize>,
}
impl StreamReply for Reply {
    type Stream = DuplexStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(
        mut self,
        outcome: ReplyOutcome,
    ) -> Result<Option<Self::Stream>, ChannelError> {
        self.replies.fetch_add(1, Ordering::SeqCst);
        // This fixture's writes fit its duplex capacity and cannot block.
        match outcome {
            ReplyOutcome::LocalChannelReady => {
                self.io.write_all(b"ready").await.unwrap();
                Ok(Some(self.io))
            }
            ReplyOutcome::Blocked | ReplyOutcome::UpstreamFailed => {
                self.io.write_all(b"refused").await.unwrap();
                self.io.shutdown().await.unwrap();
                Ok(None)
            }
        }
    }
}

#[tokio::test]
async fn accepted_stream_keeps_concrete_response_and_channel_paired_for_one_handoff() {
    let (io, mut peer) = tokio::io::duplex(64);
    let replies = Arc::new(AtomicUsize::new(0));
    let accepted = accept_stream(Reply {
        facts: support::facts(),
        io,
        replies: replies.clone(),
    });
    assert_eq!(accepted.facts().session, SessionId(1));
    let mut stream = accepted
        .respond(ReplyOutcome::LocalChannelReady)
        .await
        .unwrap()
        .unwrap();
    stream.write_all(b"business").await.unwrap();
    stream.shutdown().await.unwrap();
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"readybusiness");
    assert_eq!(replies.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refusal_responds_without_delivering_a_business_channel() {
    for outcome in [ReplyOutcome::Blocked, ReplyOutcome::UpstreamFailed] {
        let (io, mut peer) = tokio::io::duplex(64);
        let accepted = accept_stream(Reply {
            facts: support::facts(),
            io,
            replies: Arc::new(AtomicUsize::new(0)),
        });
        assert!(accepted.respond(outcome).await.unwrap().is_none());
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"refused");
    }
}

struct Handshake {
    barrier: Option<Arc<Notify>>,
    io: DuplexStream,
    replies: Arc<AtomicUsize>,
}
impl InboundHandshake for Handshake {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        if let Some(barrier) = self.barrier {
            barrier.notified().await;
        }
        let mut facts = support::facts();
        facts.inbound = context.inbound.role;
        facts.session = context.identities.allocate()?;
        Ok(AcceptedRequest::Stream(accept_stream(Reply {
            facts,
            io: self.io,
            replies: self.replies,
        })))
    }
}
struct Source(VecDeque<Handshake>);
struct Identity;
impl zero_platform_tokio::contract::context::SessionIdentitySource for Identity {
    fn allocate(&self) -> Result<SessionId, ChannelError> {
        Ok(SessionId(2))
    }
}
impl InboundSource for Source {
    type Handshake = Handshake;
    async fn next(&mut self, _: AcceptContext) -> Result<Option<Handshake>, ChannelError> {
        Ok(self.0.pop_front())
    }
}

#[tokio::test]
async fn slow_pending_handshake_does_not_block_accepting_another_request() {
    let barrier = Arc::new(Notify::new());
    let (first_io, mut first_peer) = tokio::io::duplex(64);
    let (second_io, _second_peer) = tokio::io::duplex(64);
    let replies = Arc::new(AtomicUsize::new(0));
    let mut source = erase_inbound(Source(
        [
            Handshake {
                barrier: Some(barrier),
                io: first_io,
                replies: replies.clone(),
            },
            Handshake {
                barrier: None,
                io: second_io,
                replies,
            },
        ]
        .into(),
    ));
    let context = AcceptContext {
        deadline: Instant::now() + Duration::from_secs(1),
        cancel: CancellationToken::new(),
    };
    let first = source.next(context.clone()).await.unwrap().unwrap();
    let second = source.next(context).await.unwrap().unwrap();
    let completed = timeout(
        Duration::from_secs(1),
        second.complete(HandshakeContext {
            inbound: support::inbound(),
            identities: Arc::new(Identity),
            deadline: Instant::now() + Duration::from_secs(1),
            cancel: CancellationToken::new(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let AcceptedRequest::Stream(request) = completed else {
        panic!("expected stream");
    };
    assert_eq!(request.facts().session, SessionId(2));
    drop(first);
    assert_eq!(first_peer.read(&mut [0]).await.unwrap(), 0);
}

struct NoBusinessIdentity;
impl zero_platform_tokio::contract::context::SessionIdentitySource for NoBusinessIdentity {
    fn allocate(&self) -> Result<SessionId, ChannelError> {
        panic!("control is not a business session")
    }
}
struct ControlHandshake;
struct Control(CancellationToken);
impl zero_core::inbound::InboundControlSession for Control {
    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        None
    }
    fn run(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), zero_core::Error>> + Send>>
    {
        Box::pin(async move {
            self.0.cancelled().await;
            Ok(())
        })
    }
}
impl InboundHandshake for ControlHandshake {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        Ok(AcceptedRequest::Control(Box::new(Control(context.cancel))))
    }
}
#[tokio::test]
async fn control_handoff_and_cancellation_do_not_allocate_a_business_identity() {
    let cancel = CancellationToken::new();
    let request = PendingInbound::new(ControlHandshake)
        .complete(HandshakeContext {
            inbound: support::inbound(),
            identities: Arc::new(NoBusinessIdentity),
            deadline: Instant::now() + Duration::from_secs(1),
            cancel: cancel.clone(),
        })
        .await
        .unwrap();
    let AcceptedRequest::Control(control) = request else {
        panic!("expected control");
    };
    cancel.cancel();
    timeout(Duration::from_secs(1), control.run())
        .await
        .unwrap()
        .unwrap();
}
