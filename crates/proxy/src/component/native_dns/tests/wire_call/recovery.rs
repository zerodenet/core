//! The real lifecycle report receives faults without making closure impossible.
use super::*;
use std::sync::Arc;

#[tokio::test]
async fn physically_closed_failed_dns_owner_reports_original_faults_then_restarts_in_the_same_host()
{
    tokio::time::timeout(Duration::from_secs(10), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_dns_max_originals":1,"dns":{"servers":{"wire":{"type":"udp","host":"127.0.0.1","port":peer.local_addr().unwrap().port()}},"default_server":"wire"}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let config = proxy.engine().runtime_snapshot().config().clone();
        proxy.protocols.start_components(config.clone(), end()).await.unwrap();
        proxy.protocols.commit_components(&config, || Ok(())).unwrap();
        let wire = [0x12,0x34,1,0,0,1,0,0,0,0,0,0,6,b'a',b'c',b't',b'u',b'a',b'l',7,b'e',b'x',b'a',b'm',b'p',b'l',b'e',0,0,1,0,1];
        let (call, mut request) = proxy.resolver.prepare_udp_query(&wire);
        let mut bytes = [0;512];
        proxy.protocols.drive_component_supervision(async {
            tokio::select! {
                result=peer.recv_from(&mut bytes)=>{result.unwrap();},
                result=&mut request=>panic!("query returned before its actual UDP Pending: {result:?}"),
            }
        }).await;
        let actual = proxy.resolver.native_query_observations().into_iter()
            .find(|record| record.io_pending && record.original.is_some()).unwrap();
        let native = actual.original.clone().unwrap();
        assert!(native.closed().is_none() && !actual.task.is_joined());
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let canonical = plan.component(native.owner().instance().component).unwrap();
        let failed = zero_dns::test_support::panic_native_query_worker(&proxy.resolver).unwrap();
        let failed_joined = proxy.protocols.drive_component_supervision(failed.wait()).await;
        let panic = failed_joined.result().unwrap_err();
        call.cancel();
        assert!(proxy.protocols.drive_component_supervision(request).await.is_err());
        // This is the public original Slot lifecycle consumer. Its retained
        // report is the receiver of every original fault, not a fake receipt.
        let received = proxy.protocols.drive_component_supervision(canonical.shutdown(end())).await.unwrap();
        assert!(received.is_complete(), "actual original closure: {received:?}");
        let query_joined = actual.task.try_joined().unwrap();
        let query_error = query_joined.result().unwrap_err();
        for (joined, source) in [(&failed_joined, &panic), (&query_joined, &query_error)] {
            assert!(received.failed_tasks.iter().any(|fault| fault.id == joined.id()
                && fault.class == joined.class()
                && contains_source(&fault.error, source)), "original TaskId/class/Arc must be handed off: {received:?}");
        }
        assert!(native.closed().unwrap().matches(&native));
        assert!(native.owner().closed().unwrap().matches(native.owner()));
        assert!(native.owner().users_closed().unwrap().matches(native.owner()));
        assert!(!actual.task.scope_identity().resource_open());
        let closed = call.wait_closed(end()).await.unwrap();
        assert!(closed.matches(&call) && closed.failure_accepted() && closed.cleanup_failure_accepted());
        assert!(Arc::ptr_eq(closed.failure().unwrap(), &query_error));
        assert!(has_original_io_source(query_error.as_ref(), closed.cleanup_failure().unwrap()));
        assert!(failed_joined.registration_released());
        assert!(actual.task.try_joined().unwrap().registration_released());
        // A previously delivered fault cannot make protocol_closed false on
        // every later shutdown. Unknown IO would still keep this incomplete.
        let again = proxy.protocols.drive_component_supervision(canonical.shutdown(end())).await.unwrap();
        assert!(again.is_complete() && again.failed_tasks.is_empty(), "{again:?}");
        proxy.protocols.start_components(config.clone(), end()).await.unwrap();
        proxy.protocols.commit_components(&config, || Ok(())).unwrap();
        let new_plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let fresh = new_plan.component(canonical.id).unwrap();
        assert_ne!(canonical.instance_id(), fresh.instance_id());
        let (second, mut request) = proxy.resolver.prepare_udp_query(&wire);
        let (size, address) = proxy.protocols.drive_component_supervision(async {
            tokio::select! {
                result=peer.recv_from(&mut bytes)=>result.unwrap(),
                result=&mut request=>panic!("recovered query returned before actual UDP: {result:?}"),
            }
        }).await;
        let next = proxy.resolver.native_query_observations().into_iter()
            .find(|record| record.io_pending && record.original.is_some()).unwrap();
        assert!(!next.task.scope_identity().same_scope(&actual.task.scope_identity()));
        assert!(!next.task.same_registration(&actual.task));
        let new_native = next.original.as_ref().unwrap();
        assert_eq!(new_native.owner().instance(), fresh.instance_id());
        assert!(new_native.closed().is_none());
        let answer = zero_dns::udp::build_dns_response(&bytes[..size], &[zero_traits::IpAddress::V4([203,0,113,9])]);
        peer.send_to(&answer,address).await.unwrap();
        let answer = proxy.protocols.drive_component_supervision(request).await.unwrap();
        assert_eq!(&answer[answer.len()-4..], &[203,0,113,9]);
        let second_closed = proxy.protocols.drive_component_supervision(second.wait_closed(end())).await.unwrap();
        assert!(second_closed.matches(&second) && second_closed.failure().is_none());
        assert!(second_closed.query().unwrap().same_registration(&next.task));
        proxy.protocols.shutdown_components(end()).await.unwrap();
        assert!(new_native.closed().unwrap().matches(new_native));
        assert!(new_native.owner().closed().unwrap().matches(new_native.owner()));
        assert!(new_native.owner().users_closed().unwrap().matches(new_native.owner()));
        assert!(!next.task.scope_identity().resource_open());
        // Keep the actual bounded lifecycle receiver alive through recovery.
        assert!(!received.failed_tasks.is_empty());
    }).await.unwrap();
}
pub(super) fn contains_source(
    error: &(dyn std::error::Error + 'static),
    expected: &Arc<zero_platform_tokio::contract::ChannelError>,
) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<zero_platform_tokio::contract::ChannelError>()
            .is_some_and(|source| std::ptr::eq(source, expected.as_ref()))
            || source
                .downcast_ref::<Arc<zero_platform_tokio::contract::ChannelError>>()
                .is_some_and(|source| Arc::ptr_eq(source, expected))
        {
            return true;
        }
        current = source.source();
    }
    false
}
