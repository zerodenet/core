//! Query custody is distinct from the real Source child's lifetime.
use super::*;
use crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks;
use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn local() -> (Arc<zero_dns::DnsSystem>, Vec<u8>) {
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut wire = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 9];
    wire.extend_from_slice(b"localhost");
    wire.extend_from_slice(&[0, 0, 1, 0, 1]);
    (proxy.resolver.clone(), wire)
}
#[tokio::test]
async fn local_dns_call_cannot_refund_datagram_child_before_original_source_join() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (resolver, wire) = local();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let tasks = ScopedSourceTasks::new("actual DNS Source child", 1, TaskClass::Auxiliary);
        tasks.bind(scope.registrar().downgrade()).unwrap();
        let owner = NativeObligations::new(1, 1, 1);
        owner
            .bind_source(scope.registrar().scope_identity())
            .unwrap();
        let native = owner.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut consumer = tasks.lock().await.unwrap();
        consumer
            .spawn_with_witness(move |actual| async move {
                let mut original = native.reserve(ChildKind::Dns).unwrap();
                let (call, work) = resolver.prepare_udp_query(&wire);
                original
                    .bind_dns_call(call.clone(), DnsSourceCaller::DatagramChild(actual))
                    .unwrap();
                work.await?;
                drop(original);
                assert!(send.send(call).is_ok());
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        let call = receive.await.unwrap();
        owner.settle_joined(end()).await.unwrap();
        assert_eq!(owner.0.lock().unwrap().records.len(), 1);
        assert!(
            !tasks.quiescent(),
            "body return is not the Source task's join"
        );
        scope.next_exit().await.unwrap();
        consumer.join_next().await.unwrap().unwrap().unwrap();
        owner.settle_joined(end()).await.unwrap();
        assert!(owner.0.lock().unwrap().records.is_empty());
        let closed = call.closed().unwrap();
        assert!(closed.matches(&call));
        assert!(
            closed.query().is_none() && closed.joined().is_none(),
            "actual localhost execution acquired no Query/native original"
        );
        assert!(tasks.quiescent());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn long_tcp_caller_reuses_query_quota_while_its_real_read_and_source_scope_remain_live() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (resolver, wire) = local();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connecting = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
        let (connecting, accepted) = tokio::join!(connecting, listener.accept());
        let mut reader = connecting.unwrap();
        let mut writer = accepted.unwrap().0;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let tasks = ScopedSourceTasks::new("shared DNS TCP caller", 1, TaskClass::Auxiliary);
        tasks.bind(scope.registrar().downgrade()).unwrap();
        let owner = NativeObligations::new(1, 1, 1);
        owner
            .bind_source(scope.registrar().scope_identity())
            .unwrap();
        let native = owner.clone();
        let pending = Arc::new(tokio::sync::Notify::new());
        let observed = pending.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut consumer = tasks.lock().await.unwrap();
        consumer
            .spawn_with_witness(move |actual| async move {
                for _ in 0..3 {
                    let mut original = native
                        .reserve_reclaimed(ChildKind::Dns, end())
                        .await
                        .unwrap();
                    let (call, work) = resolver.prepare_tcp_query(&wire);
                    original
                        .bind_dns_call(call.clone(), DnsSourceCaller::TcpConnection(actual.clone()))
                        .unwrap();
                    work.await?;
                    drop(original);
                    native.settle_joined(end()).await.unwrap();
                    assert!(native.0.lock().unwrap().records.is_empty());
                    assert!(call.closed().unwrap().matches(&call));
                    assert!(
                        !actual.is_joined(),
                        "query return cannot fabricate shared TCP caller join"
                    );
                }
                assert!(send.send(actual).is_ok());
                let mut byte = [0];
                let read = reader.read_exact(&mut byte);
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
                    observed.notify_one();
                    Poll::Ready(())
                })
                .await;
                read.await?;
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        let actual = receive.await.unwrap();
        pending.notified().await;
        assert!(!actual.is_joined());
        assert!(!tasks.quiescent());
        assert_eq!(consumer.len(), 1, "real shared TCP task remains charged");
        assert!(
            owner.0.lock().unwrap().records.is_empty(),
            "finished query debt is independent"
        );
        writer.write_all(&[1]).await.unwrap();
        scope.next_exit().await.unwrap();
        consumer.join_next().await.unwrap().unwrap().unwrap();
        assert!(actual.is_joined() && tasks.quiescent());
    })
    .await
    .unwrap();
}
