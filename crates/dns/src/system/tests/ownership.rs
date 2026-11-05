use super::super::*;
use std::{
    future::{poll_fn, Future},
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::Arc,
    task::Poll,
};
use tokio::time::{Duration, Instant};

fn lookup(owner: &owner::LookupOwner) -> (Arc<owner::Job>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut reader, _) = listener.accept().unwrap();
    let original = owner
        .start(move || {
            // The real original job is suspended on actual I/O before calling the
            // OS resolver. No completion, native receipt or Pending is fabricated.
            reader.read_exact(&mut [0u8; 1])?;
            ("localhost", 0).to_socket_addrs().map(|addresses| {
                addresses
                    .map(|address| ip_addr_to_ip(address.ip()))
                    .collect()
            })
        })
        .unwrap();
    (original, writer)
}

#[tokio::test]
async fn cancelled_wait_and_cleanup_retain_the_same_original_until_its_real_join() {
    let owner = owner::LookupOwner::default();
    let (job, mut writer) = lookup(&owner);
    let mut waiting = Box::pin(job.join());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert!(!job.complete());
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(owner.retained());
    assert_eq!(
        owner.start(|| Ok(vec![])).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        owner.shutdown(Instant::now()).await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(!job.complete());
    writer.write_all(&[1]).unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(job.complete());
    assert!(!owner.retained());
}

#[tokio::test]
async fn cancelled_originals_keep_capacity_until_confirmed_completion() {
    let owner = owner::LookupOwner::default();
    let mut originals = Vec::new();
    for _ in 0..16 {
        originals.push(lookup(&owner));
    }
    assert_eq!(
        owner.start(|| Ok(vec![])).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    for (_, writer) in &mut originals {
        writer.write_all(&[1]).unwrap();
    }
    owner
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(originals.iter().all(|(job, _)| job.complete()));
}

#[tokio::test]
async fn resolver_replacement_waits_for_the_default_system_original() {
    let dns = crate::DnsSystem::build(None).unwrap();
    let resolver = match &*dns.inner.read().unwrap() {
        crate::DnsSystemInner::System(backend) => match backend.as_ref() {
            crate::ResolverBackend::System(resolver) => resolver.clone(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    let (original, mut writer) = lookup(&resolver.0);
    let config: zero_config::DnsConfig = serde_json::from_value(serde_json::json!({
        "servers":{"new":{"type":"system"}}, "default_server":"new"
    }))
    .unwrap();
    let candidate = dns
        .prepare_reload_resources_with_dispatch(
            Some(&config),
            crate::compile_standalone_dispatch(Some(&config)).unwrap(),
            &Default::default(),
        )
        .unwrap();
    assert!(
        resolver.is_open(),
        "cold preparation leaves the original owner admitted"
    );
    assert_eq!(
        dns.commit_reload_resources(&candidate).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(resolver.is_open());
    assert!(dns
        .retire_reload_resources(&candidate, Instant::now())
        .await
        .is_err());
    assert!(!original.complete());
    assert_eq!(
        dns.commit_reload_resources(&candidate).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    writer.write_all(&[1]).unwrap();
    dns.retire_reload_resources(&candidate, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(original.complete());
    dns.commit_reload_resources(&candidate).unwrap();
    assert!(!resolver.is_open());
    assert_eq!(
        resolver.resolve("127.0.0.1").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    dns.shutdown_queries(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_joined_panic_remains_an_original_failure_after_reaping() {
    let owner = owner::LookupOwner::default();
    let job = owner
        .start(|| panic!("original OS resolver child"))
        .unwrap();
    assert!(job.join().await.is_err());
    assert!(owner.start(|| Ok(vec![])).is_err());
    assert!(!owner.close_idle());
    assert!(owner.retained());
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert!(owner.retained());
}

#[test]
fn registry_reclaims_unused_cold_system_candidates() {
    let owners = crate::transport_owners::TransportOwners::default();
    let previous = TokioSystemResolver::default();
    let original = Arc::downgrade(&previous.0);
    owners.register_system(previous).unwrap();
    owners
        .register_system(TokioSystemResolver::default())
        .unwrap();
    assert!(original.upgrade().is_none());
}

#[tokio::test]
async fn failed_commit_rollback_cannot_reopen_an_explicitly_revoked_original() {
    let resolver = TokioSystemResolver::default();
    assert!(resolver.close_idle());
    resolver.revoke();
    resolver.reopen_idle();
    assert_eq!(
        resolver.resolve("127.0.0.1").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    resolver
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn rollback_reopens_only_a_staged_idle_original_without_recreating_it() {
    let resolver = TokioSystemResolver::default();
    let original = resolver.0.clone();
    assert!(resolver.close_idle());
    assert!(resolver.resolve("127.0.0.1").await.is_err());
    resolver.reopen_idle();
    assert!(Arc::ptr_eq(&original, &resolver.0));
    assert_eq!(
        resolver.resolve("127.0.0.1").await.unwrap(),
        vec![IpAddress::V4([127, 0, 0, 1])]
    );
    resolver
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_default_system_snapshot_replaced_before_lookup_returns_stale_instead_of_panicking() {
    let dns = crate::DnsSystem::build(None).unwrap();
    let original = dns
        .config_generation
        .load(std::sync::atomic::Ordering::Acquire);
    let config: zero_config::DnsConfig = serde_json::from_value(serde_json::json!({
        "servers":{"new":{"type":"system"}}, "default_server":"new"
    }))
    .unwrap();
    dns.reload(Some(&config)).unwrap();
    assert_eq!(
        dns.resolve_system_coordinated("127.0.0.1", crate::DnsQueryRole::Node, original)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        dns.resolve_system_type_coordinated(
            "127.0.0.1",
            crate::message::TYPE_A,
            crate::DnsQueryRole::Node,
            original
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::Interrupted
    );
    dns.shutdown_queries(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
