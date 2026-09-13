use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::UdpSocket;
use zero_traits::EchForceQuery;
use zero_transport::tls::ech::EchConfigResolver;

use super::{apply_force, CacheKey, CacheRecord, RuntimeEchResolver, MAX_CACHE_ENTRIES};

async fn answer_once(
    material: Option<Vec<u8>>,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = calls.clone();
    let task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        let (length, peer) = socket.recv_from(&mut buffer).await.unwrap();
        task_calls.fetch_add(1, Ordering::SeqCst);
        let response = https_response(&buffer[..length], material.as_deref());
        socket.send_to(&response, peer).await.unwrap();
    });
    (format!("udp://{address}"), calls, task)
}

fn https_response(query: &[u8], material: Option<&[u8]>) -> Vec<u8> {
    let question_end = question_end(query);
    let mut response = Vec::from(&query[..question_end]);
    response[2] = 0x81;
    response[3] = 0x80;
    response[6..8].copy_from_slice(&1_u16.to_be_bytes());
    response[8..12].fill(0);
    response.extend_from_slice(&[0xc0, 0x0c, 0, 65, 0, 1]);
    response.extend_from_slice(&60_u32.to_be_bytes());
    let mut rdata = vec![0, 1, 0];
    if let Some(material) = material {
        rdata.extend_from_slice(&5_u16.to_be_bytes());
        rdata.extend_from_slice(&(material.len() as u16).to_be_bytes());
        rdata.extend_from_slice(material);
    }
    response.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    response.extend_from_slice(&rdata);
    response
}

fn question_end(message: &[u8]) -> usize {
    let mut offset = 12;
    while message[offset] != 0 {
        offset += usize::from(message[offset]) + 1;
    }
    offset + 5
}

#[tokio::test]
async fn reuses_dns_material_from_the_bounded_runtime_cache() {
    let material = vec![0, 4, 0xfe, 0x0d, 0, 0];
    let (server, calls, task) = answer_once(Some(material.clone())).await;
    let resolver = RuntimeEchResolver::new(
        Arc::new(zero_dns::DnsSystem::build(None).unwrap()),
        zero_platform_tokio::EgressInterfaceControl::default(),
    );

    for _ in 0..2 {
        let answer = resolver
            .resolve(
                server.clone(),
                "secret.example".to_owned(),
                EchForceQuery::Full,
            )
            .await
            .unwrap();
        assert_eq!(answer, Some(material.clone()));
    }
    task.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn full_rejects_cached_empty_answer_while_half_uses_plain_tls() {
    let (server, calls, task) = answer_once(None).await;
    let resolver = RuntimeEchResolver::new(
        Arc::new(zero_dns::DnsSystem::build(None).unwrap()),
        zero_platform_tokio::EgressInterfaceControl::default(),
    );

    let error = resolver
        .resolve(
            server.clone(),
            "secret.example".to_owned(),
            EchForceQuery::Full,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("required ECH"));
    assert_eq!(
        resolver
            .resolve(server, "secret.example".to_owned(), EchForceQuery::Half)
            .await
            .unwrap(),
        None
    );
    task.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn evicts_oldest_entries_at_the_cache_bound() {
    let resolver = RuntimeEchResolver::new(
        Arc::new(zero_dns::DnsSystem::build(None).unwrap()),
        zero_platform_tokio::EgressInterfaceControl::default(),
    );
    for index in 0..MAX_CACHE_ENTRIES + 40 {
        resolver.entry(super::CacheKey {
            server: format!("udp://127.0.0.1:{}", index + 1),
            query_name: "secret.example".to_owned(),
            egress_generation: 0,
        });
    }
    assert_eq!(
        resolver.cache.lock().unwrap().entries.len(),
        MAX_CACHE_ENTRIES
    );
}

#[test]
fn query_failures_never_downgrade_to_plain_tls() {
    let failed = CacheRecord {
        material: None,
        expires: std::time::Instant::now() + std::time::Duration::from_secs(60),
        failure: Some("resolver unavailable".into()),
    };
    assert!(apply_force(failed.clone(), EchForceQuery::None).is_err());
    assert!(apply_force(failed.clone(), EchForceQuery::Half).is_err());
    assert!(apply_force(failed, EchForceQuery::Full).is_err());
}

#[tokio::test]
async fn caches_half_query_failure_without_downgrading_a_none_query() {
    let resolver = RuntimeEchResolver::new(
        Arc::new(zero_dns::DnsSystem::build(None).unwrap()),
        zero_platform_tokio::EgressInterfaceControl::default(),
    );
    let server = "tcp://127.0.0.1:53".to_owned();
    let query_name = "secret.example".to_owned();

    let first = resolver
        .resolve(server.clone(), query_name.clone(), EchForceQuery::Half)
        .await
        .unwrap_err();
    assert!(first.to_string().contains("ECH DNS query failed"));
    let key = CacheKey {
        server: server.clone(),
        query_name: query_name.clone(),
        egress_generation: 0,
    };
    assert!(resolver
        .entry(key)
        .record
        .read()
        .await
        .as_ref()
        .is_some_and(|record| record.failure.is_some()));

    let cached = resolver
        .resolve(server, query_name, EchForceQuery::None)
        .await
        .unwrap_err();
    assert!(cached.to_string().contains("ECH DNS query failed"));
}
