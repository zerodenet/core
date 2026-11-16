//! Actual standard cache worker and original Scope; no native I/O is claimed.
use super::*;
use crate::encryption::{config::EncryptionConfig, EncryptionClient};
use base64::Engine;
use std::num::NonZeroUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::scope::TaskScope;

fn pair() -> (EncryptionClient, EncryptionServer) {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let server = EncryptionServer::new(
        EncryptionConfig::server(&format!(
            "mlkem768x25519plus.native.600s.100-35-35.{}",
            b64.encode([7; 32]),
        ))
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    let client = EncryptionClient::new(
        EncryptionConfig::client(&format!(
            "mlkem768x25519plus.native.0rtt.100-35-35.{}",
            b64.encode(&server.public_keys()[0]),
        ))
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    (client, server)
}
async fn exchange(client: &EncryptionClient, server: &EncryptionServer) {
    let (a, b) = tokio::io::duplex(4096);
    let ((), ()) = tokio::join!(
        async {
            let mut io = client.handshake(a).await.unwrap();
            io.write_all(b"original ticket").await.unwrap();
            io.flush().await.unwrap();
            io.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"after EOF");
        },
        async {
            let mut io = server.handshake_hosted(b).await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"original ticket");
            io.write_all(b"after EOF").await.unwrap();
            io.shutdown().await.unwrap();
        }
    );
}
async fn pending(owner: &HostedMaintenance) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !owner.sleeping.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(start_paused = true)]
async fn same_hosted_cache_resumes_rejects_replay_prunes_idle_and_joins_original_worker() {
    let (client, server) = pair();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = server.prepare_hosted(CancellationToken::new()).unwrap();
    assert!(server.check_hosted().is_err());
    assert!(owner.original.is_none());
    owner.start(&scope.registrar()).unwrap();
    let (id, witness) = owner.original.as_ref().unwrap().clone();
    pending(&owner).await;
    owner.start(&scope.registrar()).unwrap();
    assert_eq!(owner.original.as_ref().unwrap().0, id);
    exchange(&client, &server).await;
    exchange(&client, &server).await;
    assert_eq!(
        server.inner.cache.lock().unwrap().counts_for_test(),
        (1, 1),
        "full and resumed handshakes use the exact same original cache"
    );
    {
        let mut cache = server.inner.cache.lock().unwrap();
        cache.insert([1; 16], [2; 64], 60).unwrap();
        assert_eq!(cache.resume(&[1; 16], [3; 32]).unwrap(), Some([2; 64]));
        assert!(cache.resume(&[1; 16], [3; 32]).is_err());
        cache.expire_for_test();
    }
    owner.sleeping.store(false, Ordering::Release);
    tokio::time::advance(Duration::from_secs(60)).await;
    pending(&owner).await;
    assert_eq!(server.inner.cache.lock().unwrap().counts_for_test(), (0, 0));
    {
        let mut cache = server.inner.cache.lock().unwrap();
        cache.insert([5; 16], [6; 64], 600).unwrap();
        cache.resume(&[5; 16], [7; 32]).unwrap();
    }
    owner.revoke();
    assert!(server.check_hosted().is_err());
    assert!(
        !witness.is_joined(),
        "stop is not an original Scope join receipt"
    );
    let report = scope
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete() && report.failed_tasks.is_empty());
    assert!(report.exited_tasks.contains(&id));
    assert!(witness.matches_joined(&witness.try_joined().unwrap()));
    assert_eq!(
        server.inner.cache.lock().unwrap().counts_for_test(),
        (1, 1),
        "stop and actual worker join do not erase valid ticket/replay history"
    );
    assert!(owner.start(&scope.registrar()).is_err());
    assert!(
        server.start_library_maintenance().is_err(),
        "retirement cannot start a fallback library worker"
    );
}
#[tokio::test]
async fn taskless_encryption_uses_no_worker_and_cold_library_state_is_not_adopted() {
    let (_, server) = pair();
    server.start_library_maintenance().unwrap();
    assert!(server.prepare_hosted(CancellationToken::new()).is_err());
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let plain = EncryptionServer::new(
        EncryptionConfig::server(&format!(
            "mlkem768x25519plus.native.0s.100-35-35.{}",
            b64.encode([7; 32]),
        ))
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = plain.prepare_hosted(CancellationToken::new()).unwrap();
    owner.start(&scope.registrar().without_tasks()).unwrap();
    assert!(owner.original.is_none() && plain.check_hosted().is_ok());
    owner.revoke();
    let report = scope
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete() && report.exited_tasks.is_empty());
}

#[tokio::test]
async fn denied_original_task_registration_fences_cache_without_library_fallback() {
    let (_, server) = pair();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = server.prepare_hosted(CancellationToken::new()).unwrap();
    assert!(owner.start(&scope.registrar().without_tasks()).is_err());
    assert!(owner.original.is_none() && server.check_hosted().is_err());
    assert!(server.start_library_maintenance().is_err());
    let report = scope
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete() && report.exited_tasks.is_empty());
}
