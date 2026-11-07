//! Real H2 driver/socket, with its actual cancellation waker re-entering cache.
use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
use tokio::time::Instant;
struct Reenter {
    clients: Weak<tokio::sync::Mutex<Vec<DohClient>>>,
    calls: AtomicUsize,
    locked: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if self
            .clients
            .upgrade()
            .is_some_and(|clients| clients.try_lock().is_err())
        {
            self.locked.store(true, Ordering::Release);
        }
    }
}
#[derive(Debug)]
struct Binding(AtomicBool);
impl crate::DnsCarrierBinding for Binding {
    fn check_current(&self) -> io::Result<()> {
        if self.0.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original binding retired",
            ))
        }
    }
    fn revoked(&self) -> crate::DnsBindingRevokedFuture<'_> {
        // This supplied standalone binding signals no unrelated task. Its
        // check_current is the actual revalidation point exercised below.
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn actual_cached_validation_and_native_or_standalone_replacement_wake_outside_same_membership(
) {
    tokio::time::timeout(Duration::from_secs(3), async {
        for mode in 0..3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let socket = tokio::net::TcpStream::connect(address).await.unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            let (sender, connection) = h2::client::handshake(socket).await.unwrap();
            let egress = zero_platform_tokio::EgressInterfaceControl::default();
            let resolver = DohDnsResolver::new_cleartext(
                "127.0.0.1".into(),
                address.port(),
                "/dns-query".into(),
                vec![],
                egress.clone(),
            )
            .unwrap();
            let binding = Arc::new(Binding(AtomicBool::new(true)));
            let state = resolver.owner.client(
                Some(binding.clone()),
                true,
                egress.clone(),
                egress.generation(),
            );
            resolver.owner.spawn(state.clone(), connection).unwrap();
            let key = ConnectionKey {
                addr: address,
                generation: egress.generation(),
                detour: Some("same-real-binding".into()),
            };
            resolver.clients.lock().await.push(DohClient {
                key: key.clone(),
                sender,
                state: state.clone(),
            });
            // The handshake writes its preface inline. Its queued SETTINGS
            // below are sent only by this same original registered driver.
            use tokio::io::AsyncReadExt;
            let mut preface = [0; 24];
            peer.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            let mut header = [0; 9];
            peer.read_exact(&mut header).await.unwrap();
            assert_eq!(header[3], 4, "real original driver's SETTINGS frame");
            assert_eq!(&header[5..], &[0; 4]);
            let length =
                ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
            let mut settings = [0; 256];
            assert!(length <= settings.len());
            peer.read_exact(&mut settings[..length]).await.unwrap();
            assert!(!resolver.owner.drained());
            let reenter = Arc::new(Reenter {
                clients: Arc::downgrade(&resolver.clients),
                calls: AtomicUsize::new(0),
                locked: AtomicBool::new(false),
            });
            let waker = Waker::from(reenter.clone());
            let mut context = Context::from_waker(&waker);
            let cancelled = state.cancel.cancelled();
            tokio::pin!(cancelled);
            assert!(std::future::Future::poll(cancelled.as_mut(), &mut context).is_pending());
            assert!(!state.cancel.is_cancelled());
            match mode {
                0 => {
                    binding.0.store(false, Ordering::Release);
                    assert!(resolver.cached_client(&key).await.is_none());
                }
                1 => resolver.retire_cached_clients(Some(address), None).await,
                _ => {
                    resolver
                        .retire_cached_clients(Some(address), Some(key.generation))
                        .await
                }
            }
            assert!(state.cancel.is_cancelled());
            assert!(reenter.calls.load(Ordering::Acquire) > 0);
            assert!(
                !reenter.locked.load(Ordering::Acquire),
                "same original cancellation woke while clients membership was locked, mode={mode}"
            );
            assert!(resolver.clients.lock().await.is_empty());
            // Explicit standalone owner: consume the actual driver JoinSet;
            // do not manufacture a Host Scope or native-close receipt from EOF.
            resolver
                .owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(resolver.owner.drained());
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).await.unwrap();
        }
    })
    .await
    .unwrap();
}
