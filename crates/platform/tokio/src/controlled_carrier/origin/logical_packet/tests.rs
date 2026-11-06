use super::*;
use crate::contract::scope::{TaskClass, TaskScope};
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    task::Poll,
};
use tokio::io::AsyncReadExt;
use zero_core::component::{ComponentId, ResourceGeneration};

#[tokio::test]
async fn actual_software_driver_join_and_ack_never_claim_native_ancestor_close() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(NonZeroUsize::MIN));
        scope.enable_registration().unwrap();
        let registrar = scope.registrar();
        let cancel = registrar.cancellation();
        let stopped = cancel.clone();
        let (mut reader, _writer) = tokio::io::duplex(8);
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (_, witness) = registrar.spawn_tracked(TaskClass::Critical, async move {
            let mut byte = [0];
            let reading = reader.read_exact(&mut byte);
            tokio::pin!(reading);
            poll_fn(|cx| {
                assert!(reading.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            }).await;
            ready.send(()).unwrap();
            tokio::select! { _ = stopped.cancelled() => {}, _ = &mut reading => panic!("original software read must remain pending") }
            Ok(())
        }).unwrap();
        pending.await.unwrap();
        let instance = ComponentInstanceId { component: ComponentId(991), generation: ResourceGeneration(1) };
        let device = PacketDeviceIdentity::new(instance, registrar.scope_identity());
        let original = LogicalPacketProviderRegistration::from_registered(device, witness.clone(), cancel).unwrap();
        let origin = CarrierOrigin::from_logical_packet(original.clone());
        assert!(origin.driver().unwrap().same_registration(&witness));
        assert!(origin.native_origin().same_original(&origin));
        assert!(origin.closed().is_none());
        assert!(origin.owner().closed().is_none());
        assert!(origin.try_enter_packet_source().is_err(), "software task is not the native two-participant Source pause");
        scope.begin_shutdown();
        let joined = scope.next_receipt().await.unwrap();
        joined.result().unwrap();
        assert!(witness.try_joined().is_some());
        assert!(!witness.try_joined().unwrap().registration_released());
        assert!(origin.closed().is_none());
        scope.acknowledge(&joined).unwrap();
        assert!(witness.try_joined().unwrap().registration_released());
        assert!(scope.next_receipt().await.is_none());
        assert!(origin.is_retired());
        assert!(origin.closed().is_none());
        assert!(origin.owner().closed().is_none());
        assert!(origin.logical_packet().unwrap().same_owner(&original));
    }).await.unwrap();
}
