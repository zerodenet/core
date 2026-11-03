use super::*;
use std::{future::Future, task::Poll};
#[tokio::test]
async fn native_mux_datagram_cleanup_keeps_original_until_both_views_and_control_return() {
    timeout(Duration::from_secs(40),async {
        for protocol in ["vless","vmess","trojan"] {
            let server=Server::start(protocol).await;let udp=UdpEcho::start().await;
            let proxy=Proxy::from_engine_with_background(zero_engine::Engine::new(config(protocol,server.fixture.front.port())).unwrap(),true).unwrap();start_components(&proxy).await;
            let mut channel=open(&proxy,40,DatagramTargets::Fixed(udp.target())).await.unwrap();
            send_packet(&mut channel,40,udp.target(),b"owned").await;receive(&mut channel,40,udp.target(),b"owned").await;
            let registration=registration(&proxy);let observed=registration.observe().unwrap();let original=observed.slots()[0].original().clone();let driver=observed.slots()[0].driver().unwrap().1.clone();
            // Establish actual response suspension, rather than using a delay.
            {
                let pending=recv(channel.rx.as_mut());tokio::pin!(pending);
                std::future::poll_fn(|cx|{assert!(pending.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            }
            let zero_platform_tokio::contract::datagram::DatagramSession {tx,rx,mut control,..}=channel;
            {
                let closing=proxy.protocols.shutdown_components(deadline());tokio::pin!(closing);
                tokio::select! {_=driver.wait()=>{},result=&mut closing=>panic!("held UDP custody allowed cleanup: {result:?}")}
                drop(tx);
                std::future::poll_fn(|cx|{assert!(closing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
                let incomplete=control.wait_closed(Instant::now()).await.unwrap();assert!(!incomplete.is_complete());
                drop(rx);
                // Even with both data views returned, retain the real original
                // control debt until complete close and actual view return.
                std::future::poll_fn(|cx|{assert!(closing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            }
            assert_eq!(server.fixture.connections(),1);assert!(connect(&proxy,server.fixture.echo.port()).await.is_err());
            let report=control.wait_closed(deadline()).await.unwrap();assert!(report.is_complete());assert!(report.closed_io.is_empty());
            {
                let closing=proxy.protocols.shutdown_components(deadline());tokio::pin!(closing);
                // A complete logical report does not return the original
                // transferred control view. Drive real host cleanup while
                // that view is still held; it must remain pending.
                std::future::poll_fn(|cx|{assert!(closing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
                drop(control);
                closing.await.unwrap();
            }
            assert!(original.matches_closed(&original.closed().unwrap()));
            assert!(registration.closed().unwrap().matches(&registration));assert!(registration.users_closed().unwrap().matches(&registration));
            udp.close().await;server.close().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn native_mux_real_apply_waits_for_original_udp_control_before_starting_successor() {
    timeout(Duration::from_secs(40), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let udp = UdpEcho::start().await;
            let initial = config(protocol, server.fixture.front.port());
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(initial.clone()).unwrap(),
                true,
            )
            .unwrap();
            let running = proxy.spawn();
            let handle = crate::ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut channel = open(&proxy, 50, DatagramTargets::Fixed(udp.target()))
                .await
                .unwrap();
            send_packet(&mut channel, 50, udp.target(), b"old").await;
            receive(&mut channel, 50, udp.target(), b"old").await;
            let registration = registration(&proxy);
            let observed = registration.observe().unwrap();
            let original = observed.slots()[0].original().clone();
            let driver = observed.slots()[0].driver().unwrap().1.clone();
            let old_plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let old_slot = old_plan.configured("a").unwrap();
            let historical = old_slot.datagram(deadline()).await.unwrap();
            let mut candidate = initial;
            match &mut candidate.outbounds[0].protocol {
                zero_config::OutboundProtocolConfig::Vless {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Vmess {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Trojan {
                    mux_concurrency, ..
                } => *mux_concurrency = Some(3),
                _ => unreachable!(),
            }
            let replacing = tokio::spawn(async move {
                handle
                    .apply_runtime_config_and_wait(candidate, Duration::from_secs(10))
                    .await
            });
            timeout(Duration::from_secs(3), driver.wait())
                .await
                .unwrap();
            assert!(!replacing.is_finished());
            assert_eq!(server.fixture.connections(), 1);
            assert!(old_slot.datagram(deadline()).await.is_err());
            let zero_platform_tokio::contract::datagram::DatagramSession {
                tx,
                rx,
                mut control,
                ..
            } = channel;
            drop(tx);
            drop(rx);
            // Data views returning is insufficient: the retained original
            // logical control is still charged to that same physical record.
            assert!(!replacing.is_finished());
            assert_eq!(server.fixture.connections(), 1);
            assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
            // Keep the close report, then return the real control view. A
            // report alone cannot end custody of a still-held capability.
            drop(control);
            replacing.await.unwrap().unwrap();
            assert!(original.matches_closed(&original.closed().unwrap()));
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(registration.users_closed().unwrap().matches(&registration));
            let services = proxy.tcp_runtime_services();
            let next = services
                .components
                .as_ref()
                .unwrap()
                .configured("a")
                .unwrap();
            assert_ne!(old_slot.instance_id(), next.instance_id());
            assert!(old_slot.datagram(deadline()).await.is_err());
            drop(historical);
            let mut fresh = open(&proxy, 51, DatagramTargets::Fixed(udp.target()))
                .await
                .unwrap();
            send_packet(&mut fresh, 51, udp.target(), b"new").await;
            receive(&mut fresh, 51, udp.target(), b"new").await;
            assert_eq!(server.fixture.connections(), 2);
            close(fresh).await;
            running.shutdown().await.unwrap();
            udp.close().await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}
