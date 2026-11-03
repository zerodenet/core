#![cfg(feature = "http")]
use super::*;
use crate::runtime::Proxy;
use std::{
    future::{poll_fn, Future},
    io::{Read, Write},
    task::Poll,
    time::Duration,
};

#[tokio::test]
async fn completed_domain_lookup_cannot_bind_until_its_actual_scope_join_is_consumed() {
    tokio::time::timeout(Duration::from_secs(4), async {
        let proxy = Proxy::new(RuntimeConfig::parse(r#"{
            "inbounds":[{"tag":"domain","listen":{"address":"localhost","port":0},"protocol":{"type":"http"}}],
            "route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
        let config = proxy.engine().config();
        let prepared = proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(), None).unwrap().unwrap();
        let (shutdown, _) = watch::channel(false);
        let mut lease = proxy.inbound_owners.reserve_listener("domain", &config, &proxy.protocols,
            prepared, shutdown, NonZeroUsize::new(1).unwrap()).unwrap();
        let owner = lease.owner();
        let grant = Arc::clone(lease.grant.tcp_resolutions().first().unwrap());
        let (_, witness) = grant.start_resolution(&owner.preparation_tasks().unwrap()).unwrap();
        while grant.resolution_result().is_err() { tokio::task::yield_now().await; }
        assert!(!witness.is_joined(), "worker result is not an actual Scope join");
        let error = lease.bind_native().expect_err("pending actual join blocks native acquisition");
        assert_eq!(crate::transport::channel_failure(&error).unwrap().reason,
            zero_platform_tokio::contract::Reason::InvalidConfig);
        lease.resolve_native().await.unwrap();
        assert!(witness.is_joined());
        lease.bind_native().unwrap();
        drop(lease);
        proxy.inbound_owners.retire_bind_reservation_reported("domain", Instant::now() + Duration::from_secs(1), |terminal| {
            assert!(terminal.joined_tasks() >= 1);
            assert_eq!(terminal.closed_io_receipts(), 1, "the actual original listener closed");
            Ok(())
        }).await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn cancelled_domain_preparation_retains_actual_lookup_worker_until_original_scope_join() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = Proxy::new(RuntimeConfig::parse(r#"{
            "inbounds":[{"tag":"domain","listen":{"address":"localhost","port":0},"protocol":{"type":"http"}}],
            "route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
        let config = proxy.engine().config();
        let prepare = || proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(), None).unwrap().unwrap();
        let (shutdown, _) = watch::channel(false);
        let mut lease = proxy.inbound_owners.reserve_listener("domain", &config, &proxy.protocols,
            prepare(), shutdown.clone(), NonZeroUsize::new(1).unwrap()).unwrap();
        let owner = lease.owner();
        let original = owner.identity();
        let grant = Arc::clone(lease.grant.tcp_resolutions().first().unwrap());
        let gate_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut release = std::net::TcpStream::connect(gate_listener.local_addr().unwrap()).unwrap();
        let (mut gate, _) = gate_listener.accept().unwrap();
        let suspended = Arc::new(tokio::sync::Notify::new());
        let ready = suspended.clone();
        let (task, witness) = grant.start_lookup(&owner.preparation_tasks().unwrap(), move || {
            use std::net::ToSocketAddrs;
            // Keep a real system lookup in this actual production worker. The
            // extra real I/O gate makes its pending join deterministic.
            let addresses: Arc<[std::net::SocketAddr]> = ("localhost", 0).to_socket_addrs()?.collect::<Vec<_>>().into();
            gate.set_nonblocking(true)?;
            let mut byte = [0];
            match gate.read(&mut byte) {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {},
                other => panic!("real original gate must still be pending: {other:?}"),
            }
            gate.set_nonblocking(false)?;
            ready.notify_one();
            gate.read_exact(&mut byte)?;
            Ok(addresses)
        }).unwrap();
        let mut resolving = Box::pin(lease.resolve_native());
        tokio::select! {
            _ = suspended.notified() => {},
            result = &mut resolving => panic!("lookup worker remains at real pending I/O: {result:?}"),
        }
        assert!(!witness.is_joined());
        drop(resolving);
        drop(lease);
        let mut retiring = Box::pin(proxy.inbound_owners.retire_bind_reservation_reported(
            "domain", Instant::now() + Duration::from_secs(3), |terminal| {
                assert_eq!(terminal.identity(), original);
                assert!(terminal.joined_tasks() >= 1);
                assert_eq!(terminal.closed_io_receipts(), 0, "no native bind occurred while resolving");
                Ok(())
            }));
        poll_fn(|cx| match retiring.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => panic!("original worker has not exited or joined: {result:?}"),
        }).await;
        assert!(!witness.is_joined());
        assert!(proxy.inbound_owners.identities().contains(&original));
        assert!(proxy.inbound_owners.reserve_listener("domain", &config, &proxy.protocols,
            prepare(), shutdown.clone(), NonZeroUsize::new(1).unwrap()).is_err(), "unconfirmed original blocks a new generation");
        release.write_all(b"x").unwrap();
        retiring.await.unwrap();
        assert!(witness.is_joined());
        assert_eq!(witness.try_joined().unwrap().id(), task);
        assert!(!proxy.inbound_owners.identities().contains(&original));
        let replacement = proxy.inbound_owners.reserve_listener("domain", &config, &proxy.protocols,
            prepare(), shutdown, NonZeroUsize::new(1).unwrap()).unwrap();
        assert_ne!(replacement.owner().identity(), original);
        drop(replacement);
        proxy.inbound_owners.retire_bind_reservation_reported("domain", Instant::now() + Duration::from_secs(1), |_| Ok(())).await.unwrap();
    }).await.unwrap();
}
