//! Ordinary taskless protocol prefix -> same actual Mieru pool and driver.
use super::*;
use std::future::Future;
use zero_platform_tokio::contract::CloseReason;

#[tokio::test]
async fn ordinary_taskless_protocol_prefixes_transfer_their_actual_original_to_the_shared_mieru_pool(
) {
    tokio::time::timeout(Duration::from_secs(45), async {
        let protocols = [
            "vless",
            "vmess",
            "trojan",
            #[cfg(feature = "shadowsocks")]
            "shadowsocks",
        ];
        for protocol in protocols {
            let mut prefix = if protocol == "shadowsocks" {
                crate::runtime::MuxPrefix::shadowsocks("aes-128-gcm", "a").await
            } else {
                crate::runtime::MuxPrefix::start(protocol, "a").await
            };
            match &mut prefix.node.protocol {
                zero_config::OutboundProtocolConfig::Vless {
                    mux_concurrency,
                    xudp_concurrency,
                    ..
                } => {
                    *mux_concurrency = None;
                    *xudp_concurrency = None;
                }
                zero_config::OutboundProtocolConfig::Vmess {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Trojan {
                    mux_concurrency, ..
                } => *mux_concurrency = None,
                _ => {}
            }
            let b = Server::new(true).await;
            let c = Server::new(false).await;
            let mut config = config([b.address, b.address, c.address]);
            config.outbounds[0] = prefix.node.clone();
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut held = loan(&proxy).await;
            held.io.write_all(b"same original").await.unwrap();
            let mut answer = [0; 13];
            held.io.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"same original");
            let final_original = held.owner.original().clone();
            let middle = final_original.parent().unwrap();
            let root = middle.parent().unwrap().clone();
            assert!(root.parent().is_none());
            assert!(
                root.driver().is_none(),
                "taskless protocol must not invent a driver"
            );
            assert!(root.closed().is_none());
            assert_ne!(root.instance(), final_original.instance());
            assert_eq!(
                middle.instance(),
                final_original.instance(),
                "both Mieru hops use the same actual pool"
            );
            assert_eq!(b.accepts.load(Ordering::SeqCst), 1);
            assert_eq!(c.accepts.load(Ordering::SeqCst), 1);
            assert!(proxy.engine().active_sessions().is_empty());
            let mut pending = [0u8; 1];
            let mut read = Box::pin(held.io.read(&mut pending));
            std::future::poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(read);
            match &mut config.outbounds[0].protocol {
                zero_config::OutboundProtocolConfig::Vless { id, .. }
                | zero_config::OutboundProtocolConfig::Vmess { id, .. } => {
                    *id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into()
                }
                zero_config::OutboundProtocolConfig::Trojan { password, .. }
                | zero_config::OutboundProtocolConfig::Shadowsocks { password, .. } => {
                    *password = "replacement-password".into()
                }
                _ => unreachable!(),
            }
            let replacement = tokio::spawn(async move {
                handle
                    .apply_runtime_config_and_wait(config, Duration::from_secs(10))
                    .await
            });
            final_original.driver().unwrap().wait().await;
            assert!(
                !replacement.is_finished(),
                "actual retained logical/control custody blocks replacement"
            );
            held.owner.cancel(CloseReason::Replaced);
            assert!(!held
                .owner
                .close(tokio::time::Instant::now() + Duration::from_millis(20))
                .await
                .unwrap()
                .is_complete());
            drop(held.io);
            assert!(held.owner.close(deadline()).await.unwrap().is_complete());
            assert!(held.owner.closed().unwrap().matches(&held.owner));
            replacement.await.unwrap().unwrap();
            assert!(root.closed().unwrap().matches(&root));
            assert!(root.closed().unwrap().had_native_acquisition());
            assert!(root.owner().closed().unwrap().matches(root.owner()));
            running.shutdown().await.unwrap();
            b.close().await;
            c.close().await;
            prefix.close().await;
        }
    })
    .await
    .unwrap();
}
