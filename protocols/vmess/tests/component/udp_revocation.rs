//! Caller revocation returns real protocol views; native close stays separate.
use super::*;
use std::time::Duration;
use zero_core::{component::ComponentId, request::Destination, Address};
use zero_platform_tokio::{
    contract::lifecycle::ChannelControl, ControlledTcpStream, TcpRelayStream, TokioSocket,
};

#[tokio::test]
async fn revoked_caller_returns_real_udp_views_before_the_original_native_close() {
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(2);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let caller = CancellationToken::new();
    let (io, mut native) = ControlledTcpStream::new(TokioSocket::new(client), caller.clone())
        .unwrap_or_else(|_| panic!("the actual socket has no prior controller"));
    let (_owner, admission) = SessionOwner::new(ComponentId(915), &caller);
    let target = Destination::new(Address::Ipv4([127, 0, 0, 1]), 53);
    let (_, state) = crate::shared::prepare_request(
        &target.address,
        target.port,
        &[7; 16],
        crate::VmessCipher::Aes128Gcm,
        crate::shared::CMD_UDP,
    )
    .unwrap();
    let stream = crate::stream::VmessAeadStream::outbound(TcpRelayStream::new(io), state).unwrap();
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            stream: Some(stream),
            views: 2,
        }),
        changed: Notify::new(),
        cancel: caller.child_token(),
        caller_cancel: caller.clone(),
        admission,
        session: None,
        association: AssociationId(1),
        observer: None,
    });
    let limits = DatagramLimits {
        max_payload: 65507,
        targets: DatagramTargets::Fixed(target),
        queued_bytes: 65537,
    };
    let tx = Tx::new(shared.clone(), limits);
    let rx = Rx::new(shared.clone());
    let mut control = Control::new(shared.clone());
    caller.cancel();
    let held = control
        .wait_closed(tokio::time::Instant::now())
        .await
        .unwrap();
    assert!(!held.is_complete(), "actual transferred views remain held");
    assert!(shared.state.lock().unwrap().stream.is_some());
    assert!(held.closed_io.is_empty());
    drop(tx);
    drop(rx);
    let returned = control.wait_closed(deadline()).await.unwrap();
    assert!(returned.is_complete(), "{returned:?}");
    assert_eq!(returned.released_resources.len(), 1);
    assert!(
        returned.closed_io.is_empty(),
        "no invented native close receipt"
    );
    assert!(shared.state.lock().unwrap().stream.is_none());
    assert!(
        native.is_cancelled(),
        "stop remains separate from actual close"
    );
    let closed = native.wait_closed(deadline()).await.unwrap();
    assert!(closed.is_complete(), "{closed:?}");
    assert_eq!(closed.closed_io.len(), 1, "same retained native controller");
    assert!(native
        .wait_closed(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
    drop(peer);
}
