use super::*;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

fn push_record(wire: &mut Vec<u8>, size: usize) {
    wire.extend_from_slice(&[23, 3, 3]);
    wire.extend_from_slice(&((size - 5) as u16).to_be_bytes());
    wire.resize(wire.len() + size - 5, 0);
}

#[test]
fn post_handshake_parser_keeps_complete_application_record_lengths() {
    let mut wire = Vec::new();
    push_record(&mut wire, 64);
    push_record(&mut wire, 96);
    wire.extend_from_slice(&[23, 3, 3, 0, 90]);
    assert_eq!(io::post_handshake_lengths(&wire).unwrap(), vec![64, 96]);
}

#[test]
fn post_handshake_parser_rejects_more_than_the_record_bound() {
    let mut wire = Vec::new();
    for _ in 0..=64 {
        push_record(&mut wire, 22);
    }
    assert!(io::post_handshake_lengths(&wire).is_err());
}

#[test]
fn alpn_key_matches_the_three_official_probe_classes() {
    assert_eq!(AlpnClass::from_offered(&[]), AlpnClass::None);
    assert_eq!(
        AlpnClass::from_offered(&["http/1.1".to_owned()]),
        AlpnClass::Http1
    );
    assert_eq!(
        AlpnClass::from_offered(&["h2".to_owned(), "http/1.1".to_owned()]),
        AlpnClass::H2
    );
}

#[tokio::test]
async fn probe_failure_keeps_the_conservative_ccs_limit() {
    let profile = Profile {
        endpoint: zero_traits::FallbackEndpoint::Tcp {
            server: "unreachable.test".into(),
            port: 443,
        },
        proxy_protocol: 0,
        upload: zero_transport::handshake_target::RateLimit::default(),
        download: zero_transport::handshake_target::RateLimit::default(),
    };
    let connector = zero_transport::handshake_target::Connector::new(|_| {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "probe failure",
            ))
        })
    });

    assert_eq!(
        probe(&profile, &connector, "unreachable.test", AlpnClass::None).await,
        Detection::default()
    );
}

#[tokio::test]
async fn recording_starts_at_the_client_finished_flight() {
    let (inner, mut peer) = tokio::io::duplex(1024);
    let capture = Arc::new(Mutex::new(io::Capture::default()));
    let mut recording = io::RecordingIo {
        inner,
        reads: capture.clone(),
    };

    peer.write_all(b"before").await.unwrap();
    let mut scratch = [0; 6];
    recording.read_exact(&mut scratch).await.unwrap();
    recording.write_all(&[22, 3, 1, 0, 1, 0]).await.unwrap();
    peer.write_all(b"handshake").await.unwrap();
    let mut scratch = [0; 9];
    recording.read_exact(&mut scratch).await.unwrap();
    assert!(capture.lock().unwrap().bytes.is_empty());

    recording.write_all(ccs::CCS_RECORD).await.unwrap();
    let mut record = Vec::new();
    push_record(&mut record, 64);
    peer.write_all(&record).await.unwrap();
    let mut received = vec![0; record.len()];
    recording.read_exact(&mut received).await.unwrap();

    let capture = capture.lock().unwrap();
    assert!(capture.active);
    assert_eq!(capture.bytes, record);
}

#[tokio::test]
async fn registry_reuses_a_completed_probe_and_reload_cancels_an_inflight_probe() {
    let registry = Registry::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let expected = Detection {
        post_handshake_lengths: vec![64],
        max_ccs_records: 16,
    };
    let first = registry
        .detect_cached("same".into(), {
            let calls = calls.clone();
            let expected = expected.clone();
            move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                expected
            }
        })
        .await;
    let second = registry
        .detect_cached("same".into(), || async { Detection::default() })
        .await;
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let started = Arc::new(tokio::sync::Notify::new());
    let waiting = tokio::spawn({
        let registry = registry.clone();
        let started = started.clone();
        async move {
            registry
                .detect_cached("waiting".into(), || async move {
                    started.notify_one();
                    std::future::pending().await
                })
                .await
        }
    });
    started.notified().await;
    registry.retire();
    assert_eq!(waiting.await.unwrap(), Detection::default());
    let expected_after_reload = expected.clone();
    assert_eq!(
        registry
            .detect_cached("new-generation".into(), move || async move {
                expected_after_reload
            })
            .await,
        expected
    );
}

#[tokio::test]
async fn dropping_the_last_registry_owner_cancels_access_waiters() {
    let registry = Registry::default();
    let access = registry.access();
    let started = Arc::new(tokio::sync::Notify::new());
    let waiting = tokio::spawn({
        let started = started.clone();
        async move {
            access
                .detect_cached("waiting".into(), || async move {
                    started.notify_one();
                    std::future::pending().await
                })
                .await
        }
    });
    started.notified().await;
    drop(registry);
    assert_eq!(waiting.await.unwrap(), Detection::default());
}

struct WriteTap<S> {
    inner: S,
    writes: Arc<Mutex<Vec<u8>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteTap<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteTap<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(count)) => {
                self.writes.lock().unwrap().extend_from_slice(&buf[..count]);
                Poll::Ready(Ok(count))
            }
            result => result,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn ccs_probe_injects_records_before_the_client_finished_flight() {
    let cert = rcgen::generate_simple_self_signed(vec!["probe.test".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let (client, server_io) = tokio::io::duplex(64 * 1024);
    let server_task = tokio::spawn(async move {
        let _ = tokio_rustls::TlsAcceptor::from(Arc::new(server))
            .accept(server_io)
            .await;
    });
    let writes = Arc::new(Mutex::new(Vec::new()));
    let tapped = WriteTap {
        inner: client,
        writes: writes.clone(),
    };
    let config = ztls::handshake::Tls13Config {
        server_name: "probe.test".into(),
        server_verifier: Some(verifier),
        ..Default::default()
    };

    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        ccs::drive_ccs_probe(tapped, config),
    )
    .await;
    server_task.abort();
    let expected = [ccs::CCS_RECORD, ccs::CCS_RECORD].concat();
    let writes = writes.lock().unwrap();
    let position = writes
        .windows(expected.len())
        .position(|window| window == expected)
        .expect("the probe must inject two CCS records");
    assert!(matches!(
        writes.get(position + expected.len()),
        Some(20 | 23)
    ));
}
