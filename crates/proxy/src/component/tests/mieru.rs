//! The actual Proxy constructor, registry, shared pool, host apply and I/O.
mod budget;
#[cfg(feature = "socks5")]
mod cross_owner;
#[cfg(all(feature = "wireguard", feature = "socks5"))]
mod device;
#[cfg(feature = "dns")]
mod dns;
mod fixtures;
mod reject;
#[cfg(all(
    feature = "dns",
    feature = "vless",
    feature = "vmess",
    feature = "trojan"
))]
mod stateless;
mod udp_handoff;
use crate::{Proxy, ProxyHandle};
use fixtures::*;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn ordinary_proxy_compiles_mieru_roles_over_the_same_actual_shared_pool() {
    let address = "127.0.0.1:1".parse().unwrap();
    let proxy = Proxy::new(config([address; 3])).unwrap();
    let services = proxy.tcp_runtime_services();
    let components = services.components.as_ref().unwrap();
    for tag in ["a", "b", "c", "unused"] {
        let slot = components
            .configured(tag)
            .expect("ordinary Mieru component");
        assert_eq!(slot.recipe.spec().dependencies.len(), 1);
        assert_eq!(
            slot.recipe.spec().roles,
            vec![
                zero_platform_tokio::contract::component::RoleKind::Stream,
                zero_platform_tokio::contract::component::RoleKind::Datagram,
            ]
        );
    }
}

#[tokio::test]
async fn actual_proxy_mieru_network_change_closes_original_prefix_before_new_carriers_and_waits_for_retained_control(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let a = Server::new(true).await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let config = config([a.address, b.address, c.address]);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        let mut held = loan(&proxy).await;
        let prior = held.owner.original().clone();
        let native = prior.native_origin().clone();
        proxy.egress_interface_control().invalidate_network();
        assert!(
            try_loan(&proxy).await.is_err(),
            "old users forbid new actual acquisitions"
        );
        let mut current = Some(&prior);
        while let Some(original) = current {
            tokio::time::timeout(
                Duration::from_secs(3),
                original.driver().expect("actual Mieru driver").wait(),
            )
            .await
            .unwrap();
            current = original.parent();
        }
        assert!(
            try_loan(&proxy).await.is_err(),
            "original retirement still cannot bypass the retained logical control"
        );
        assert!(native.driver().expect("actual Mieru driver").is_joined());
        let original_close = native
            .closed()
            .expect("original native close and actual transferred-view return");
        assert!(original_close.matches(&native) && original_close.had_native_acquisition());
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        }
        drop(held.io);
        assert!(
            try_loan(&proxy).await.is_err(),
            "I/O return does not settle its retained protocol controller"
        );
        assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        let mut fresh = loan(&proxy).await;
        let successor = fresh.owner.original().clone();
        assert_eq!(
            prior.instance(),
            successor.instance(),
            "a network transition reuses the same one actual pool owner"
        );
        assert!(!native
            .driver()
            .expect("actual Mieru driver")
            .same_registration(
                successor
                    .native_origin()
                    .driver()
                    .expect("actual Mieru driver")
            ));
        fresh.io.write_all(b"network").await.unwrap();
        fresh.io.read_exact(&mut [0; 7]).await.unwrap();
        drop(fresh.io);
        assert!(fresh.owner.close(deadline()).await.unwrap().is_complete());
        running.shutdown().await.unwrap();
        let final_root = successor.native_origin();
        assert!(final_root.closed().unwrap().matches(final_root));
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_mieru_three_hop_prefix_retains_originals_and_replacement_waits_for_logical_control(
) {
    replacement(false).await;
}
#[tokio::test]
async fn actual_proxy_mieru_three_hop_packet_prefix_and_tcp_udp_share_the_one_real_pool() {
    replacement(true).await;
}
async fn replacement(udp: bool) {
    tokio::time::timeout(Duration::from_secs(35), async {
        let a = Server::new(true).await;
        let b = if udp {
            Server::packet(true).await
        } else {
            Server::new(true).await
        };
        let c = Server::new(false).await;
        let mut config = config([a.address, b.address, c.address]);
        if udp {
            if let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
                &mut config.outbounds[1].protocol
            {
                *transport = zero_config::MieruTransport::Udp;
            }
        }
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
        let mut service = service(&proxy).await;
        service.write_all(b"actual Proxy prefix").await.unwrap();
        let mut bytes = [0; 19];
        service.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"actual Proxy prefix");
        drop(service);
        let mut datagram = packet(&proxy).await;
        let frame = zero_core::packet::Datagram {
            association: zero_core::component::AssociationId(200),
            address: destination(),
            payload: b"udp-prefix".to_vec().into(),
        };
        zero_platform_tokio::contract::datagram::send(
            datagram.tx.as_mut(),
            datagram.control.as_mut(),
            &frame,
            &datagram.limits,
        )
        .await
        .unwrap();
        let response = std::future::poll_fn(|cx| datagram.rx.as_mut().poll_recv(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.payload.as_ref(), b"udp-prefix");
        drop(datagram.tx);
        drop(datagram.rx);
        assert!(datagram
            .control
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        drop(datagram.control);
        let mut held = loan(&proxy).await;
        let original = held.owner.original().clone();
        let middle = original.parent().expect("actual middle donor").clone();
        let root = middle.parent().expect("actual native donor").clone();
        assert!(root.parent().is_none());
        assert_eq!(original.instance(), middle.instance());
        assert_eq!(
            middle.instance(),
            root.instance(),
            "one actual shared pool generation"
        );
        assert!(original
            .driver()
            .expect("actual Mieru driver")
            .scope_identity()
            .same_scope(&root.driver().expect("actual Mieru driver").scope_identity()));
        assert!(!original
            .driver()
            .expect("actual Mieru driver")
            .same_registration(middle.driver().expect("actual Mieru driver")));
        assert!(!middle
            .driver()
            .expect("actual Mieru driver")
            .same_registration(root.driver().expect("actual Mieru driver")));
        for server in [&a, &b, &c] {
            assert_eq!(
                server.accepts.load(Ordering::SeqCst),
                1,
                "logical reuse must not create a second pool"
            );
        }
        assert!(
            proxy.engine().active_sessions().is_empty(),
            "background work must not invent a business session"
        );

        let mut changed = config;
        if let zero_config::OutboundProtocolConfig::Mieru { password, .. } =
            &mut changed.outbounds[3].protocol
        {
            *password = "unused credential replaced".into();
        }
        let replacing = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(changed, Duration::from_secs(10))
                .await
        });
        tokio::time::timeout(
            Duration::from_secs(3),
            original.driver().expect("actual Mieru driver").wait(),
        )
        .await
        .unwrap();
        assert!(
            !replacing.is_finished(),
            "old transferred loan still owns its real user debt"
        );
        let pending = held
            .owner
            .close(tokio::time::Instant::now() + Duration::from_millis(20))
            .await
            .unwrap();
        assert!(!pending.is_complete());
        drop(held.io);
        assert!(
            !replacing.is_finished(),
            "returning I/O alone cannot close the logical controller"
        );
        assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        assert!(held.owner.closed().unwrap().matches(&held.owner));
        replacing.await.unwrap().unwrap();
        for prior in [&original, &middle, &root] {
            assert!(prior.driver().expect("actual Mieru driver").is_joined());
            assert!(prior.owner().closed().unwrap().matches(prior.owner()));
            assert!(prior.owner().users_closed().unwrap().matches(prior.owner()));
        }
        let mut fresh = loan(&proxy).await;
        let successor = fresh.owner.original().clone();
        assert_ne!(successor.instance(), original.instance());
        fresh.io.write_all(b"successor").await.unwrap();
        fresh.io.read_exact(&mut [0; 9]).await.unwrap();
        drop(fresh.io);
        assert!(fresh.owner.close(deadline()).await.unwrap().is_complete());
        assert!(
            !successor
                .native_origin()
                .driver()
                .expect("actual Mieru driver")
                .is_joined(),
            "returning one logical session leaves its shared donor running"
        );
        running.shutdown().await.unwrap();
        assert!(successor.driver().expect("actual Mieru driver").is_joined());
        assert!(successor
            .native_origin()
            .owner()
            .closed()
            .unwrap()
            .matches(successor.native_origin().owner()));
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "vmess")]
mod transition;
