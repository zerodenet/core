use super::*;
#[tokio::test]
async fn native_mux_interrupted_cleanup_retains_original_and_blocks_rebuild_until_real_logical_release(
) {
    timeout(Duration::from_secs(30),async {
        for protocol in ["vless","vmess","trojan"] {
            let server=Server::start(protocol).await;
            let proxy=Proxy::from_engine_with_background(zero_engine::Engine::new(config(protocol,server.fixture.front.port())).unwrap(),true).unwrap();
            start_components(&proxy).await;
            let mut held=connect(&proxy,server.fixture.echo.port()).await.unwrap(); echo(&mut held).await;
            let registration=registration(&proxy);
            let observation=registration.observe().unwrap();
            let original=observation.slots()[0].original().clone();
            let driver=observation.slots()[0].driver().unwrap().1.clone();
            // Interrupt the actual host cleanup after its real Scope driver has
            // exited, while a real transferred logical stream is still held.
            {
                let closing=proxy.protocols.shutdown_components(deadline());
                tokio::pin!(closing);
                tokio::select! {
                    _=driver.wait()=>{},
                    result=&mut closing=>panic!("cleanup completed before held stream returned: {result:?}"),
                }
                std::future::poll_fn(|cx| {
                    use std::future::Future;
                    assert!(closing.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                }).await;
            }
            assert_eq!(server.fixture.connections(),1);
            assert!(connect(&proxy,server.fixture.echo.port()).await.is_err(),"old selected role must not reopen through compatibility");
            drop(held);
            proxy.protocols.shutdown_components(deadline()).await.unwrap();
            assert!(original.matches_closed(&original.closed().unwrap()));
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(registration.users_closed().unwrap().matches(&registration));
            server.close().await;
        }
    }).await.unwrap();
}
