#![cfg(feature = "split_http")]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{SplitHttpOptions, SplitHttpRange, SplitHttpXmux};
use zero_transport::{profile::OwnedSplitHttpProfile, split_http::*};
fn profile(mode: &str, xmux: SplitHttpXmux) -> OwnedSplitHttpProfile {
    OwnedSplitHttpProfile {
        host: None,
        path: "/mux/".into(),
        mode: mode.into(),
        options: SplitHttpOptions {
            xmux,
            sc_max_each_post_bytes: SplitHttpRange::new(1024, 1024),
            sc_min_posts_interval_ms: SplitHttpRange::new(1, 1),
            ..Default::default()
        },
    }
}
fn factory(config: OwnedSplitHttpProfile, h2: bool) -> (XhttpCarrierFactory, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let registry = SplitHttpRegistry::new();
    let factory: XhttpCarrierFactory = Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        let config = config.clone();
        let registry = registry.clone();
        Box::pin(async move {
            let (a, b) = tokio::io::duplex(65536);
            tokio::spawn(async move {
                let incoming = accept_xhttp_connection(b, &config, &registry);
                while let Some(stream) = incoming.accept().await {
                    tokio::spawn(async move {
                        let (mut read, mut write) = tokio::io::split(stream);
                        let _ = tokio::io::copy(&mut read, &mut write).await;
                    });
                }
            });
            let stream = zero_platform_tokio::TcpRelayStream::new(a);
            Ok(if h2 {
                XhttpCarrier::Http2(stream)
            } else {
                XhttpCarrier::Http1(stream)
            })
        })
    });
    (factory, calls)
}
async fn echo(stream: &mut XhttpStream, size: usize) {
    let payload = vec![91; size];
    let (mut read, mut write) = tokio::io::split(stream);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let send = async {
            write.write_all(&payload).await.unwrap();
            write.flush().await.unwrap();
        };
        let recv = async {
            let mut reply = vec![0; size];
            read.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, payload);
        };
        tokio::join!(send, recv);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn shared_h2_survives_one_stream_drop_and_retirement() {
    let config = profile(
        "stream-one",
        SplitHttpXmux {
            max_connections: SplitHttpRange::new(1, 1),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (factory, calls) = factory(config.clone(), true);
    let mut first = pool.connect(factory.clone(), &config).await.unwrap();
    let mut second = pool.connect(factory.clone(), &config).await.unwrap();
    tokio::join!(echo(&mut first, 70001), echo(&mut second, 70001));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(first);
    echo(&mut second, 13).await;
    pool.retire();
    let mut third = pool.connect(factory.clone(), &config).await.unwrap();
    tokio::join!(echo(&mut second, 101), echo(&mut third, 103));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn packet_request_limit_rotates_client_without_replaying_or_losing_bytes() {
    let config = profile(
        "packet-up",
        SplitHttpXmux {
            h_max_request_times: SplitHttpRange::new(3, 3),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (factory, calls) = factory(config.clone(), true);
    let mut stream = pool.connect(factory, &config).await.unwrap();
    echo(&mut stream, 20001).await;
    assert!(calls.load(Ordering::SeqCst) >= 7);
}
#[tokio::test]
async fn http1_reuses_upload_connection_only_after_response_eof() {
    let config = profile(
        "packet-up",
        SplitHttpXmux {
            max_connections: SplitHttpRange::new(1, 1),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (factory, calls) = factory(config.clone(), false);
    let mut stream = pool.connect(factory, &config).await.unwrap();
    echo(&mut stream, 20001).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn logical_reuse_limit_starts_new_group_and_keeps_old_stream_alive() {
    let config = profile(
        "stream-one",
        SplitHttpXmux {
            c_max_reuse_times: SplitHttpRange::new(2, 2),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (factory, calls) = factory(config.clone(), true);
    let mut first = pool.connect(factory.clone(), &config).await.unwrap();
    echo(&mut first, 1).await;
    let mut second = pool.connect(factory.clone(), &config).await.unwrap();
    echo(&mut second, 2).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut third = pool.connect(factory, &config).await.unwrap();
    echo(&mut third, 3).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    echo(&mut first, 4).await;
}

#[tokio::test]
async fn http1_stream_modes_allow_upload_while_response_is_open() {
    for mode in ["stream-up", "stream-one"] {
        let config = profile(
            mode,
            SplitHttpXmux {
                max_connections: SplitHttpRange::new(1, 1),
                ..Default::default()
            },
        );
        let pool = XhttpClientPool::new(config.options.xmux);
        let (factory, _) = factory(config.clone(), false);
        let mut stream = pool.connect(factory, &config).await.unwrap();
        echo(&mut stream, 131073).await;
    }
}

#[tokio::test(start_paused = true)]
async fn reusable_age_retires_group_without_interrupting_its_response() {
    let config = profile(
        "stream-one",
        SplitHttpXmux {
            h_max_reusable_secs: SplitHttpRange::new(1, 1),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (factory, calls) = factory(config.clone(), true);
    let mut old = pool.connect(factory.clone(), &config).await.unwrap();
    echo(&mut old, 17).await;
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    let mut new = pool.connect(factory, &config).await.unwrap();
    tokio::join!(echo(&mut old, 19), echo(&mut new, 23));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn pool_drop_releases_factory_after_last_logical_stream() {
    let config = profile(
        "stream-one",
        SplitHttpXmux {
            max_connections: SplitHttpRange::new(1, 1),
            ..Default::default()
        },
    );
    let pool = XhttpClientPool::new(config.options.xmux);
    let (base, _) = factory(config.clone(), true);
    let marker = Arc::new(());
    let weak = Arc::downgrade(&marker);
    let factory: XhttpCarrierFactory = Arc::new(move || {
        let _ = &marker;
        base()
    });
    let mut stream = pool.connect(factory.clone(), &config).await.unwrap();
    echo(&mut stream, 7).await;
    drop(factory);
    drop(pool);
    assert!(weak.upgrade().is_some());
    drop(stream);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(weak.upgrade().is_none());
}
