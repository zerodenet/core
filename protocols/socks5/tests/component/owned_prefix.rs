//! Real Service/prefix acquisition retains originals before protocol handoff.
#[path = "owned_prefix/fixtures.rs"]
mod fixtures;
use fixtures::*;
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{
    network::UpstreamTransport, stream::ConnectRequest, CloseReason,
};

async fn request(io: &mut zero_platform_tokio::TcpRelayStream) -> (zero_core::Address, u16) {
    let command = socks5::Socks5Inbound.accept_command(io).await.unwrap();
    let socks5::Socks5Request::Connect(session) = command else {
        panic!("TCP CONNECT")
    };
    (session.target, session.port)
}
async fn accepted(io: &mut zero_platform_tokio::TcpRelayStream) {
    socks5::Socks5Inbound
        .send_success_response(io)
        .await
        .unwrap();
}

#[tokio::test]
async fn native_root_service_retains_its_original_until_its_view_returns() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let remote = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut io = zero_platform_tokio::TcpRelayStream::new(io);
            assert_eq!(request(&mut io).await, (target().address, target().port));
            accepted(&mut io).await;
            let (mut read, mut write) = tokio::io::split(io);
            let _ = tokio::io::copy(&mut read, &mut write).await;
        });
        let mut owner = Installed::new(10, endpoint(address)).await;
        let mut connected = owner
            .instance
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: target(),
                },
                context(owner.root(address)),
            )
            .await
            .unwrap();
        connected.io.write_all(b"root").await.unwrap();
        let mut bytes = [0; 4];
        connected.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"root");
        let registration = owner.owner();
        let observed = registration.observe().unwrap();
        assert_eq!(observed.slots().len(), 1);
        let original = observed.slots()[0].original().clone();
        assert!(observed.slots()[0].native().is_some());
        assert!(observed.slots()[0].driver().is_none());
        let denied = owner
            .instance
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: target(),
                },
                context(owner.root(address)),
            )
            .await
            .err()
            .expect("the held Service must charge the same one-original limit");
        assert_eq!(
            denied.reason,
            zero_platform_tokio::contract::Reason::Capacity
        );
        owner.instance.lifecycle.revoke(CloseReason::Shutdown);
        assert!(!owner
            .instance
            .lifecycle
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_millis(15))
            .await
            .unwrap()
            .is_complete());
        drop(connected.io);
        assert!(owner
            .instance
            .lifecycle
            .shutdown(end())
            .await
            .unwrap()
            .is_complete());
        assert!(original.matches_closed(&original.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(owner.scope.shutdown(end()).await.is_complete());
        remote.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_service_borrows_the_real_prefix_and_returns_both_originals() {
    borrowed(false, false).await;
}
#[tokio::test]
async fn cancelled_service_handshake_retains_the_same_prefix_controller_for_cleanup() {
    borrowed(true, false).await;
}
#[tokio::test]
async fn dropped_carrier_handshake_retains_the_same_prefix_controller_for_cleanup() {
    borrowed(true, true).await;
}
async fn borrowed(cancel: bool, carrier: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        let suffix=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=suffix.local_addr().unwrap();
        let donor_listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let donor_address=donor_listener.local_addr().unwrap();
        let (parsed,wait_parsed)=tokio::sync::oneshot::channel();let (release,wait_release)=tokio::sync::oneshot::channel();
        let remote=tokio::spawn(async move {
            let(io,_)=suffix.accept().await.unwrap();let mut io=zero_platform_tokio::TcpRelayStream::new(io);assert_eq!(request(&mut io).await,(target().address,target().port));
            parsed.send(()).unwrap();
            if cancel {let _=wait_release.await;} else {accepted(&mut io).await;let(mut read,mut write)=tokio::io::split(io);let _=tokio::io::copy(&mut read,&mut write).await;}
        });
        let forwarding=tokio::spawn(async move {
            let(io,_)=donor_listener.accept().await.unwrap();let mut io=zero_platform_tokio::TcpRelayStream::new(io);assert_eq!(request(&mut io).await,(endpoint(address).address,address.port()));
            let mut outgoing=tokio::net::TcpStream::connect(address).await.unwrap();accepted(&mut io).await;let _=tokio::io::copy_bidirectional(&mut io,&mut outgoing).await;
        });
        let mut donor=Installed::new(10,endpoint(donor_address)).await;let mut receiver=Installed::new(20,endpoint(address)).await;
        let pending=Arc::new(AtomicBool::new(false));
        let prefix=Arc::new(Prefix {role:donor.instance.roles.stream.as_ref().unwrap().clone(),root:donor.root(donor_address),selected:binding(vec![donor.role]),endpoint:endpoint(address),original:Mutex::new(None),pending:pending.clone()});
        let ctx=context(UpstreamTransport::new(prefix.selected.clone(),prefix.clone()));let cancellation=ctx.cancel.clone();
        let role=receiver.instance.roles.stream.as_ref().unwrap();
        let open=async {
            if carrier {
                let connected=role.connect_carrier(ConnectRequest {destination:target()},ctx).await?;
                // This branch is cancelled before a handoff; a successful loan
                // would need its own explicit caller and cannot be dropped here.
                panic!("unexpected successful pre-delivery carrier: {:?}",connected.peer_confirmation)
            } else {role.connect(ConnectRequest {destination:target()},ctx).await}
        };
        let mut open = Box::pin(open);
        let mut connected=None;
        if cancel {
            tokio::select! {result=&mut open=>panic!("handshake returned before withheld reply: {}",result.is_ok()),parsed=wait_parsed=>parsed.unwrap()}
            pending.store(false,Ordering::Release);
            std::future::poll_fn(|cx| {assert!(open.as_mut().poll(cx).is_pending());std::task::Poll::Ready(())}).await;
            assert!(pending.load(Ordering::Acquire),"actual awaited native read returned Pending");
            if carrier {drop(open);} else {cancellation.cancel();assert_eq!(open.await.err().unwrap().reason,zero_platform_tokio::contract::Reason::Cancelled);}
        } else {
            let mut io=open.await.unwrap();io.io.write_all(b"owned").await.unwrap();let mut bytes=[0;5];io.io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"owned");connected=Some(io);
        }
        let donor_origin=prefix.original.lock().unwrap().as_ref().unwrap().clone();
        let registration=receiver.owner();let observed=registration.observe().unwrap();assert_eq!(observed.slots().len(),1);
        let last=&observed.slots()[0];let original=last.original().clone();
        assert!(last.borrowed_origin().unwrap().same_original(&donor_origin));assert!(last.native().is_none());assert!(last.driver().is_none());
        receiver.instance.lifecycle.revoke(CloseReason::Shutdown);
        drop(connected);
        assert!(receiver.instance.lifecycle.shutdown(end()).await.unwrap().is_complete());
        assert!(original.matches_closed(&original.closed().unwrap()));
        let closed=donor_origin.closed().expect("same native controller close receipt");assert!(closed.matches(&donor_origin));assert!(closed.had_native_acquisition());
        assert!(registration.closed().unwrap().matches(&registration));assert!(registration.users_closed().unwrap().matches(&registration));
        donor.instance.lifecycle.revoke(CloseReason::Shutdown);assert!(donor.instance.lifecycle.shutdown(end()).await.unwrap().is_complete());
        assert!(receiver.scope.shutdown(end()).await.is_complete());assert!(donor.scope.shutdown(end()).await.is_complete());
        let _=release.send(());remote.await.unwrap();forwarding.await.unwrap();
    }).await.unwrap();
}
