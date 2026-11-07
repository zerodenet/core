//! Same original ICMP provider coverage without a fabricated native handle.
use super::*;

#[tokio::test]
async fn icmp_declaration_is_covered_only_by_its_installed_original_host_owner() {
    use zero_core::component::{ComponentInstanceId, ResourceGeneration};
    use zero_platform_tokio::contract::scope::TaskScope;
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let resources = HostResources::new_for_instance(
        ComponentInstanceId {
            component: ComponentId(529),
            generation: ResourceGeneration(1),
        },
        &[ResourceDeclaration::NativeIcmp { max_sockets: one() }],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let grant = resources.icmp_probe_grant().unwrap();
    assert_eq!(
        grant
            .bind("127.0.0.1".parse().unwrap(), None)
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    let cold = resources.io_evidence();
    assert_eq!(cold.declarations[0].provider, DeclarationProvider::Icmp);
    assert_eq!(cold.icmp, Some(false));
    assert!(!cold.physical_confirmed());
    let _access = resources.activate(deadline()).unwrap();
    let report = resources.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "no ICMP original was acquired, so no native close is invented"
    );
    assert_eq!(
        grant
            .bind("127.0.0.1".parse().unwrap(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    let closed = resources.io_evidence();
    assert!(closed.admission_closed && closed.physical_confirmed());
    let joined = scope.shutdown(deadline()).await;
    assert!(joined.is_complete() && joined.exited_tasks.is_empty());
}
