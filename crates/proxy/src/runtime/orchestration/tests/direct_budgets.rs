//! Ordinary Direct configuration owns the actual original and replacement.
use crate::{Proxy, ProxyHandle};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::component::ResourceDeclaration;
async fn ready(proxy: &Proxy) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.inbound_owners.owner("entry") {
                owner.wait_ready().await.unwrap();
                if proxy
                    .protocols
                    .current_components(&proxy.engine().config())
                    .is_some()
                {
                    return owner;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
async fn connect(address: std::net::SocketAddr, target: std::net::SocketAddr) -> TcpStream {
    let mut io = TcpStream::connect(address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut reply = [0; 2];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&target.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    io
}
async fn live(front: &mut TcpStream, back: &mut TcpStream) {
    front.write_all(b"same original").await.unwrap();
    let mut bytes = [0; 13];
    back.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same original");
    back.write_all(&bytes).await.unwrap();
    front.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same original");
}
#[tokio::test]
async fn actual_direct_limits_keep_live_wire_and_replace_only_after_original_close() {
    timeout(Duration::from_secs(25), async {
        let config=RuntimeConfig::parse(r#"{"runtime":{"direct_max_originals":1,"direct_max_tasks":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"bounded","protocol":{"type":"direct"}}],"route":{"final":{"type":"route","outbound":"bounded"}}}"#).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.clone().spawn();
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        let source=ready(&proxy).await;let address=*source.original_listener_address.get().unwrap();
        let plan=proxy.protocols.current_components(&proxy.engine().config()).unwrap();
        let old=plan.direct(Some("bounded")).unwrap();
        assert_eq!(old.recipe.spec().task_limit.unwrap().get(),1);
        assert!(matches!(old.recipe.spec().resources.as_slice(),[ResourceDeclaration::ControlledDirectStream{max_sockets,..}] if max_sockets.get()==1));
        let echo=TcpListener::bind("127.0.0.1:0").await.unwrap();let target=echo.local_addr().unwrap();
        let (front,back)=tokio::join!(connect(address,target),echo.accept());let mut front=front;let mut back=back.unwrap().0;
        live(&mut front,&mut back).await;
        let original=old.original_carrier_owner(0).unwrap();assert_eq!(original.limit().get(),1);
        assert_eq!(original.observe().unwrap().retained(),1);
        let mut next=config.clone();next.runtime.direct_max_originals=Some(2);next.runtime.direct_max_tasks=Some(2);
        assert!(handle.apply_runtime_config_and_wait(next.clone(),Duration::from_secs(5)).await.is_err());
        assert_eq!(proxy.engine().config().runtime.effective_direct_max_originals(),1);
        live(&mut front,&mut back).await;
        front.shutdown().await.unwrap();back.shutdown().await.unwrap();let mut tail=Vec::new();front.read_to_end(&mut tail).await.unwrap();assert!(tail.is_empty());
        drop(front);drop(back);
        timeout(Duration::from_secs(5),async {while !proxy.engine().active_sessions().is_empty(){tokio::task::yield_now().await;}}).await.unwrap();
        handle.apply_runtime_config_and_wait(next,Duration::from_secs(5)).await.unwrap();
        assert!(old.shutdown(Instant::now()+Duration::from_secs(5)).await.unwrap().is_complete());
        let plan=proxy.protocols.current_components(&proxy.engine().config()).unwrap();let new=plan.direct(Some("bounded")).unwrap();
        assert_ne!(old.instance_id(),new.instance_id());
        assert_eq!(new.original_carrier_owner(0).unwrap().limit().get(),2);
        assert_eq!(new.recipe.spec().task_limit.unwrap().get(),2);
        let (front,back)=tokio::join!(connect(address,target),echo.accept());let mut front=front;let mut back=back.unwrap().0;
        live(&mut front,&mut back).await;drop(front);drop(back);
        running.shutdown().await.unwrap();
        assert!(new.shutdown(Instant::now()+Duration::from_secs(5)).await.unwrap().is_complete());
        let closed=source.terminal_for_test().expect("actual listener native/users and Scope receipt");
        assert!(closed.joined_tasks()>0 && closed.closed_io_receipts()>0);
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
