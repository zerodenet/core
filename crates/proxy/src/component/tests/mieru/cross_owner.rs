//! Actual taskless SOCKS original -> Mieru middle -> Mieru final ownership.
use super::*;
use std::sync::Arc;
mod donor;

#[tokio::test]
async fn actual_proxy_cross_owner_taskless_original_retires_transitive_pool_before_replacement() {
    replacement(false).await;
}
#[tokio::test]
async fn actual_proxy_multiple_taskless_prefix_owners_preserve_the_one_original_and_replacement_debts(
) {
    replacement(true).await;
}
async fn replacement(taskless_middle: bool) {
    tokio::time::timeout(Duration::from_secs(35), async {
        let a = donor::Donor::new().await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let middle_donor = donor::Donor::new().await;
        let mut config = config([a.address, if taskless_middle { middle_donor.address } else { b.address }, c.address]);
        let socks = zero_config::RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"a","protocol":{{"type":"socks5","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}}],"route":{{"final":{{"type":"direct"}}}}}}"#,a.address.port())).unwrap();
        config.outbounds[0] = socks.outbounds[0].clone();
        if taskless_middle {
            config.outbounds[1] = socks.outbounds[0].clone();
            config.outbounds[1].tag = "b".into();
            if let zero_config::OutboundProtocolConfig::Socks5 { port, .. } = &mut config.outbounds[1].protocol { *port = middle_donor.address.port(); }
        }
        let mut separate = socks.outbounds[0].clone();
        separate.tag = "unrelated-socks".into();
        config.outbounds.push(separate);
        let proxy = Proxy::from_engine_with_background(zero_engine::Engine::new(config.clone()).unwrap(), true).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5)).await.unwrap();
        let mut held = loan(&proxy).await;
        held.io.write_all(b"cross-owner").await.unwrap();
        let mut bytes = [0; 11];
        held.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"cross-owner");
        let original = held.owner.original().clone();
        let middle = original.parent().unwrap().clone();
        let root = middle.parent().unwrap().clone();
        if taskless_middle {
            assert!(middle.driver().is_none(), "the middle original has no independent driver");
            assert_ne!(middle.instance(), root.instance());
            assert!(middle.native_origin().same_original(&root));
        }
        assert!(root.parent().is_none());
        if !taskless_middle { assert_eq!(middle.instance(), original.instance()); }
        assert_ne!(root.instance(), original.instance());
        assert!(root.driver().is_none(), "stateless native custody has no driver to invent");
        assert!(root.closed().is_none());
        assert_eq!(a.accepts.load(Ordering::SeqCst), 1);
        let plan = proxy.tcp_runtime_services().components.as_ref().unwrap().clone();
        let changed_tag = if taskless_middle { "b" } else { "a" };
        let donor_slot = plan.configured(changed_tag).unwrap();
        let final_slot = plan.configured("c").unwrap();
        assert!(donor_slot.recipe.spec().task_limit.is_none());
        assert!(proxy.engine().active_sessions().is_empty());
        let separate = config.outbounds.last_mut().unwrap();
        if let zero_config::OutboundProtocolConfig::Socks5 { password, .. } = &mut separate.protocol {
            *password = Some("unrelated change".into());
        }
        handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5)).await.unwrap();
        assert!(Arc::ptr_eq(&final_slot, &proxy.tcp_runtime_services().components.as_ref().unwrap().configured("c").unwrap()),
            "an unused owner must not advance the active shared pool");
        assert!(root.closed().is_none());
        // A credential-only donor change must propagate into its actual shared
        // consumers even though neither Mieru endpoint or credential changed.
        if let zero_config::OutboundProtocolConfig::Socks5 { password, .. } = &mut config.outbounds[usize::from(taskless_middle)].protocol {
            *password = Some("changed".into());
        } else { panic!("SOCKS5 configuration"); }
        let replacing = tokio::spawn(async move { handle.apply_runtime_config_and_wait(config, Duration::from_secs(10)).await });
        original.driver().unwrap().wait().await;
        assert!(!replacing.is_finished(), "real retained loan control blocks candidate start");
        assert!(root.owner().closed().is_none(), "an incomplete consumer also blocks donor owner completion");
        assert!(!held.owner.close(tokio::time::Instant::now() + Duration::from_millis(20)).await.unwrap().is_complete());
        assert_eq!(a.accepts.load(Ordering::SeqCst), 1);
        assert_eq!(if taskless_middle { middle_donor.accepts.load(Ordering::SeqCst) } else { b.accepts.load(Ordering::SeqCst) }, 1);
        drop(held.io);
        assert!(!replacing.is_finished());
        assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        replacing.await.unwrap().unwrap();
        assert!(root.closed().unwrap().matches(&root));
        assert!(root.closed().unwrap().had_native_acquisition());
        if taskless_middle {
            let returned = middle.closed().expect("same middle borrowed original closed");
            assert!(returned.matches(&middle));
            assert!(!returned.had_native_acquisition(), "returning the middle loan cannot mint native I/O acquisition");
            assert!(root.owner().closed().is_none(), "unchanged donor owner stays live after its old original closed");
        } else {
            assert!(root.owner().closed().unwrap().matches(root.owner()));
            assert!(root.owner().users_closed().unwrap().matches(root.owner()));
        }
        let next = proxy.tcp_runtime_services().components.as_ref().unwrap().clone();
        assert_ne!(next.configured(changed_tag).unwrap().instance_id(), donor_slot.instance_id());
        assert_ne!(next.configured("c").unwrap().instance_id(), final_slot.instance_id());
        let mut fresh = loan(&proxy).await;
        fresh.io.write_all(b"new").await.unwrap();
        fresh.io.read_exact(&mut [0; 3]).await.unwrap();
        assert!(fresh.owner.original().native_origin().driver().is_none());
        assert_eq!(a.accepts.load(Ordering::SeqCst), 2);
        drop(fresh.io);
        assert!(fresh.owner.close(deadline()).await.unwrap().is_complete());
        running.shutdown().await.unwrap();
        assert!(root.owner().closed().unwrap().matches(root.owner()));
        a.close().await;
        b.close().await;
        c.close().await;
        middle_donor.close().await;
    }).await.unwrap();
}
