use super::*;
use std::{future::Future, task::Poll};
use tokio::net::TcpListener;

#[tokio::test]
async fn native_mux_cancelled_tls_preparation_retains_original_without_inventing_a_driver_exit() {
    timeout(Duration::from_secs(20),async {
        for protocol in ["vless","vmess","trojan"] {
            let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy=Proxy::from_engine_with_background(zero_engine::Engine::new(config(protocol,listener.local_addr().unwrap().port())).unwrap(),true).unwrap();
            let running=start_components(&proxy).await;
            let registration=registration(&proxy);
            let original;
            let auxiliary;
            {
                let opening=connect(&proxy,9);
                tokio::pin!(opening);
                let (mut peer,_)=tokio::select! {
                    accepted=listener.accept()=>accepted.unwrap(),
                    result=&mut opening=>panic!("open completed before TLS peer response: {}",result.err().unwrap_or_default()),
                };
                let mut header=[0;5];
                tokio::select! {
                    result=peer.read_exact(&mut header)=>{result.unwrap();},
                    result=&mut opening=>panic!("open completed before actual TLS ClientHello: {}",result.err().unwrap_or_default()),
                }
                assert_eq!(header[0],22);
                assert_eq!(header[1],3);
                let size=u16::from_be_bytes([header[3],header[4]]) as usize;
                let mut hello=vec![0;size];
                tokio::select! {
                    result=peer.read_exact(&mut hello)=>{result.unwrap();},
                    result=&mut opening=>panic!("open completed during ClientHello: {}",result.err().unwrap_or_default()),
                }
                assert_eq!(hello[0],1);
                std::future::poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending(),"real TLS response read must be pending");Poll::Ready(())}).await;
                let observation=registration.observe().unwrap();
                assert_eq!(observation.slots().len(),1);
                let slot=&observation.slots()[0];
                assert!(slot.driver().is_none());
                auxiliary=slot.auxiliary().expect("same original real preparation task").clone();
                assert!(!auxiliary.is_joined());
                assert!(slot.native().is_some());
                original=slot.original().clone();
                assert!(original.closed().is_none());
                // This scope drops the real awaited opening, not a fabricated probe.
            }
            assert!(original.closed().is_none(),"cancelling preparation cannot issue original close evidence");
            timeout(Duration::from_secs(3),async {
                loop {
                    let closed=original.closed().is_some_and(|closed|original.matches_closed(&closed));
                    let joined=auxiliary.try_joined().is_some_and(|joined|joined.registration_released());
                    let returned=registration.observe().is_some_and(|state|state.slots().is_empty());
                    if closed && joined && returned {break;}
                    tokio::task::yield_now().await;
                }
            }).await.expect("actual cancelled setup must settle without next dial or component shutdown");
            proxy.protocols.shutdown_components(deadline()).await.unwrap();
            let proof=original.closed().unwrap();
            assert!(original.matches_closed(&proof) && proof.had_native_acquisition());
            assert!(registration.observe().is_none_or(|observation| observation.slots().is_empty()),"the no-driver original has actually retired");
            assert!(registration.closed().unwrap().matches(&registration));
            running.shutdown().await.unwrap();
        }
    }).await.unwrap();
}
