//! Cancel the real reply wait while its actual blocking job and opened native
//! file are retained. Neither waiter Drop nor the primary's exit proves closure.
#![cfg(feature = "vless")]
use super::*;
use crate::runtime::Proxy;
use std::{future::Future, sync::Condvar, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{oneshot, watch, Notify},
    time::{timeout, Duration},
};
use zero_config::RuntimeConfig;
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
    entered: Notify,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[tokio::test]
async fn cancelled_file_reply_retains_actual_job_original_close_and_scope_before_replacement() {
    exercise(false).await;
}
#[tokio::test]
async fn real_file_job_failure_is_published_without_losing_original_close_progress() {
    exercise(true).await;
}
async fn exercise(panic_job: bool) {
    timeout(Duration::from_secs(15),async {
        let directory=tempfile::tempdir().unwrap();let root=directory.path().canonicalize().unwrap();let path=root.join("body");std::fs::write(&path,b"original bytes").unwrap();
        let proxy=Proxy::new(RuntimeConfig::parse(r#"{"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":"11111111-2222-3333-4444-555555555555"}]}}],"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
        let config=proxy.engine().config();let prepared=proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(),None).unwrap().unwrap();
        let (stop,_)=watch::channel(false);let one=std::num::NonZeroUsize::new(1).unwrap();
        let mut lease=proxy.inbound_owners.reserve_listener("first",&config,&proxy.protocols,prepared,stop.clone(),one).unwrap();lease.bind_native().unwrap();
        let (_definition,view,installed)=lease.finish().unwrap().promote_definition().unwrap();
        let NativeListenerView::Tcp(listener)=view else {panic!("original TCP grant")};let address=listener.local_addr().unwrap();let owner=installed.owner();
        let files=FileTrees::new(&ResourceDeclaration::FileTree {root:root.clone(),max_sessions:one},owner.accepted_registrar().scope_identity(),owner.task_access().cancellation()).unwrap();
        files.start(Instant::now()+Duration::from_secs(3)).await.unwrap();
        let gate=Arc::new(Gate {released:Mutex::new(false),changed:Condvar::new(),entered:Notify::new()});let _release=Release(gate.clone());
        let hooked=gate.clone();*files.before.lock().unwrap()=Some(Arc::new(move |operation| {
            if matches!(operation,zero_platform_tokio::file::tree::FileTreeOperation::Read {..}) {
                hooked.entered.notify_one();let mut released=hooked.released.lock().unwrap();while !*released {released=hooked.changed.wait(released).unwrap();}drop(released);if panic_job {panic!("actual file blocking worker failure");}
            }
        }));
        let (send,receive)=oneshot::channel();let send=Arc::new(Mutex::new(Some(send)));let provider=files.clone();
        let request=crate::runtime::listener_loop::TcpListenerLoopRequest {
            runtime_factory:crate::runtime::route_runtime::InboundRouteRuntimeFactory::new(crate::runtime::route_runtime::SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),"first".into()),
            protocol_name:"audited-file-owner",listener:zero_platform_tokio::TokioListener::from_controlled(listener).unwrap(),shutdown:stop.subscribe(),
            handler:move |runtime:crate::runtime::route_runtime::InboundRouteRuntime,mut io:zero_platform_tokio::TokioSocket| {
                let provider=provider.clone();let root=root.clone();let path=path.clone();let send=send.clone();
                async move {let mut files=provider.open(runtime.accepted_caller().unwrap(),root).map_err(std::io::Error::other)?;
                    files.open(path).await?;send.lock().unwrap().take().unwrap().send(files).ok().unwrap();
                    io.write_all(b"ready").await?;let _=io.read(&mut[0]).await;Ok::<(),EngineError>(())}
            }
        };
        owner.spawn_driver(crate::runtime::listener_loop::run_owned_tcp_listener_loop(crate::runtime::listener_loop::OwnedTcpLoop {request,users:crate::runtime::listener_loop::AcceptedUsersPlan::PrimaryOnly})).unwrap();owner.seal_listener_installation();
        let business=async {
            let mut client=TcpStream::connect(address).await.unwrap();let mut ready=[0;5];client.read_exact(&mut ready).await.unwrap();assert_eq!(&ready,b"ready");
            let mut view=receive.await.unwrap();let original=files.issued.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert_eq!(original.owned_originals(),Some(1));
            let mut pending=Box::pin(view.read_at(0,1));std::future::poll_fn(|cx|{assert!(pending.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            gate.entered.notified().await;
            std::future::poll_fn(|cx|{assert!(pending.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            drop(pending);drop(view);stop.send_replace(true);
            let closed=timeout(Duration::from_millis(50),proxy.inbound_owners.confirm_shutdown("first",Instant::now()+Duration::from_secs(3))).await;
            assert!(!matches!(closed,Ok(Ok(Some(_)))),"an original native job is still pending");
            assert_eq!(original.owned_originals(),Some(1),"same original remains installed");assert_eq!(files.permits.available_permits(),0,"Drop cannot release native admission");
            assert!(proxy.inbound_owners.open_bind_admission().is_err());
            gate.release();drop(client);original
        };
        let (observed,original)=tokio::join!(owner.observe_driver(),business);assert_eq!(observed.is_err(),panic_job);
        let receipt=proxy.inbound_owners.confirm_shutdown("first",Instant::now()+Duration::from_secs(3)).await.unwrap().unwrap();
        assert_eq!(receipt.terminal().failure().is_some(),panic_job,"{:?}",receipt.terminal().failure());assert_eq!(receipt.terminal().joined_tasks(),3);assert_eq!(receipt.terminal().closed_io_receipts(),3);
        assert_eq!(original.owned_originals(),Some(0));assert!(original.execute(zero_platform_tokio::file::tree::FileTreeOperation::Read {offset:0,length:1}).is_err());
        assert_eq!(original.close_originals().unwrap().closed_handles(),0,"the real provider already consumed its one original receipt");assert_eq!(files.permits.available_permits(),1);
        assert!(proxy.inbound_owners.open_bind_admission().is_err());proxy.inbound_owners.release_closed(receipt,|_|Ok(())).unwrap();
    }).await.unwrap();
}
