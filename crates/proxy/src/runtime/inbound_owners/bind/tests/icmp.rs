//! No native ICMP permission is needed to verify cold/revoked authority gates.
use super::*;
use zero_platform_tokio::contract::{Reason, component::ResourceDeclaration};

#[tokio::test]
async fn canonical_icmp_grant_is_same_instance_cold_and_revoked_before_native_io() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let inbound = &config.inbounds[0];
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(inbound.clone(), None)
        .unwrap()
        .unwrap();
    let (shutdown, _) = watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            &inbound.tag,
            &config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let actual = lease.owner();
    actual
        .install_icmp_probe(
            &ResourceDeclaration::NativeIcmp {
                max_sockets: NonZeroUsize::new(64).unwrap(),
            },
            proxy.protocols.runtime_host().original_budgets(),
        )
        .unwrap();
    lease.bind_native().unwrap();
    let (operation, view, installed) = lease.finish().unwrap().promote_native().unwrap();
    drop(operation);
    drop(view); // The native UDP owner remains retained in the same resource graph.
    let grant = installed
        .icmp_probe_grant()
        .expect("explicit canonical ICMP declaration");
    assert_eq!(installed.owner().identity(), actual.identity());
    assert_eq!(
        grant
            .bind("127.0.0.1".parse().unwrap(), None)
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    actual.task_access().request_stop();
    assert_eq!(
        grant
            .bind("127.0.0.1".parse().unwrap(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    actual.seal_listener_installation();
    proxy
        .inbound_owners
        .shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let terminal = actual.terminal_for_test().unwrap();
    assert_eq!(terminal.joined_tasks(), 0);
    assert_eq!(terminal.closed_io_receipts(), 1); // UDP only: no ICMP FD was acquired.
    assert_eq!(
        grant
            .bind("127.0.0.1".parse().unwrap(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
}
