//! Cold denial does not fabricate a native grant or a registered task receipt.
use super::*;
use crate::component::catalog::ComponentCatalog;
use tokio::time::{Duration, Instant};
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    scope::{TaskClass, TaskScope},
};
#[tokio::test]
async fn cold_or_foreign_binding_is_rejected_without_native_authority() {
    let catalog = ComponentCatalog::default();
    let instance = ComponentInstanceId {
        component: catalog.allocate_id().unwrap(),
        generation: ResourceGeneration(1),
    };
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let actual_task = scope
        .registrar()
        .spawn(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    let declaration = ResourceDeclaration::BoundOutboundDatagram {
        max_sockets: NonZeroUsize::new(1).unwrap(),
        max_bindings: NonZeroUsize::new(1).unwrap(),
    };
    let provider = HostBoundDatagramResources::new(
        instance,
        actual_task,
        &declaration,
        CancellationToken::new(),
    )
    .unwrap();
    let own = provider.issuer.issue(0);
    let foreign = DatagramBindingIssuer::new(instance).issue(0);
    assert_ne!(
        own, foreign,
        "same numeric/owner value cannot replay a different private issuer"
    );
    for binding in [own, foreign] {
        let request = BoundDatagramRequest {
            binding,
            peer: "127.0.0.1:9".parse().unwrap(),
            expected_network_generation: NetworkGeneration(0),
        };
        let error = provider
            .open_bound_outbound_datagram(request.clone())
            .unwrap_err();
        assert_eq!(error.reason, Reason::InvalidConfig);
        assert!(!error.retry_permitted());
        assert!(provider.select_bound_outbound_datagram(request).is_err());
    }
    assert_eq!(provider.state().owner.occupied(), 0);
    let report = provider.shutdown_io().unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    let report = scope
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(report.is_complete());
    assert!(report.exited_tasks.contains(&actual_task));
}
