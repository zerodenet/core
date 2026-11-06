//! Explicit hosted Scope utility: no native Source/Packet provider is minted.
use super::*;
use crate::runtime::inbound_operation::raw_ip::native_obligations::{ChildKind, NativeObligations};
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    task::Poll,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};

#[tokio::test]
async fn pre_admission_native_quota_retains_actual_primary_until_host_ack_then_same_scope_reuses() {
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let instance = proxy.protocols.allocate_resource_identity().unwrap();
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let cancel = CancellationToken::new();
    let (mut main_read, mut main_write) = tokio::io::duplex(1);
    let (main_ready, main_pending) = tokio::sync::oneshot::channel();
    let (main_id, main) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let read = main_read.read_u8();
            tokio::pin!(read);
            let mut ready = Some(main_ready);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(ready) = ready.take() {
                        ready.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(io_error)?;
            Ok(())
        })
        .unwrap();
    main_pending.await.unwrap();
    let native = NativeObligations::new(1, 0, 0);
    let mut permit = native.reserve(ChildKind::Tcp).unwrap();
    let binding = Arc::new(HostPeerBinding::new(
        instance,
        cancel.clone(),
        registrar.downgrade(),
    ));
    let actual = binding.clone();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (ready, pending) = tokio::sync::oneshot::channel();
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            actual.wait_bound().await?;
            permit.bind_caller(actual)?;
            permit.enter()?;
            // No Engine session admission is issued in this actual utility call.
            let phase = permit.registration().begin_session_admission()?;
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut ready = Some(ready);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(ready) = ready.take() {
                        ready.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(io_error)?;
            drop(phase);
            Ok(())
        })
        .unwrap();
    binding.bind(id, witness.clone());
    pending.await.unwrap();
    assert_eq!(
        native.reserve(ChildKind::Tcp).err().unwrap().reason,
        Reason::Capacity
    );
    writer.write_u8(1).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    assert!(receipt.result().is_ok());
    binding.joined(id, receipt.class(), None);
    let actual = witness.try_joined().unwrap();
    assert!(actual.result().is_ok());
    assert!(!actual.registration_released());
    let end = Instant::now() + Duration::from_secs(5);
    let rejected = native
        .reserve_reclaimed(ChildKind::Tcp, end)
        .await
        .err()
        .unwrap();
    assert_eq!(rejected.reason, Reason::Capacity);
    assert!(
        !main.is_joined(),
        "the actual original Scope main remains at real IO Pending"
    );
    scope.acknowledge(&receipt).unwrap();
    assert!(witness.try_joined().unwrap().registration_released());
    let next = native.reserve_reclaimed(ChildKind::Tcp, end).await.unwrap();
    drop(next); // This exact new cold reservation never enters native work.
    assert!(!main.is_joined());
    assert!(!cancel.is_cancelled());
    main_write.write_u8(1).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), main_id);
    assert!(receipt.result().is_ok());
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
fn io_error(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source)
}
