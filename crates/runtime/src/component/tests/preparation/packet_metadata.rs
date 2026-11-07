//! Cold typed resource identity only. There is no started provider or fake IO.
use super::*;
use std::num::NonZeroUsize;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::{ResourceDeclaration, ResourceOwner};
struct SourceDescription(AtomicUsize);
struct MetadataDescription;
impl ComponentFactory for MetadataDescription {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        let mut spec = SourceDescription(AtomicUsize::new(0)).describe(&());
        spec.resources.truncate(1);
        spec
    }
    fn build(&self, _: Arc<()>, _: BuildContext) -> Result<ComponentInstance, BuildError> {
        panic!("cold metadata test cannot build or acquire an original")
    }
}

impl ComponentFactory for SourceDescription {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from(&b"same-declared-packet-source"[..]),
            task_limit: NonZeroUsize::new(2),
            resources: vec![
                ResourceDeclaration::DatagramListener {
                    accepted: None,
                    destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
                    max_bind_attempts: NonZeroUsize::MIN,
                    max_pending_sends: NonZeroUsize::MIN,
                },
                ResourceDeclaration::PacketDatagramReads {
                    datagram: 0,
                    workspaces: [NonZeroUsize::new(65536).unwrap(); 2],
                    control_workspace: None,
                    supplemental: Default::default(),
                },
            ],
        }
    }
    fn build(&self, _: Arc<()>, _: BuildContext) -> Result<ComponentInstance, BuildError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("cold metadata identity preparation cannot build or acquire a Source")
    }
}
#[test]
fn metadata_only_capacity_changes_do_not_resurrect_an_earlier_packet_resource_identity() {
    let catalog = ComponentCatalog::default();
    let mut allocate = || catalog.allocate_id();
    let factory = Arc::new(SourceDescription(AtomicUsize::new(0)));
    let original =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let mut first = ResourcePreparation::new(&mut allocate).with_candidate(&original);
    let id = first
        .share("packet.metadata", factory.clone(), Arc::new(()))
        .unwrap();
    let identities = first.identities().unwrap();
    drop(first);
    let mut same =
        ResourcePreparation::with_identities(&mut allocate, &identities).with_candidate(&original);
    assert_eq!(
        same.share("packet.metadata", factory.clone(), Arc::new(()))
            .unwrap(),
        id
    );
    drop(same);
    let mut changed_bytes = original.clone();
    changed_bytes.runtime.packet_driver_metadata_total_max_bytes = Some(
        original
            .runtime
            .effective_packet_driver_metadata_total_max_bytes()
            + 1,
    );
    let mut bytes = ResourcePreparation::with_identities(&mut allocate, &identities)
        .with_candidate(&changed_bytes);
    assert_ne!(
        bytes
            .share("packet.metadata", factory.clone(), Arc::new(()))
            .unwrap(),
        id
    );
    drop(bytes);
    let mut changed_allocations = original.clone();
    changed_allocations
        .runtime
        .packet_driver_metadata_total_max_allocations = Some(
        original
            .runtime
            .effective_packet_driver_metadata_total_max_allocations()
            + 1,
    );
    let mut allocations = ResourcePreparation::with_identities(&mut allocate, &identities)
        .with_candidate(&changed_allocations);
    assert_ne!(
        allocations
            .share("packet.metadata", factory.clone(), Arc::new(()))
            .unwrap(),
        id
    );
    assert_eq!(factory.0.load(Ordering::SeqCst), 0);
}

#[test]
fn non_packet_listener_metadata_policy_changes_its_actual_cold_resource_identity() {
    let catalog = ComponentCatalog::default();
    let mut allocate = || catalog.allocate_id();
    let original =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let factory = Arc::new(MetadataDescription);
    let mut first = ResourcePreparation::new(&mut allocate).with_candidate(&original);
    let id = first
        .share("listener.metadata", factory.clone(), Arc::new(()))
        .unwrap();
    let identities = first.identities().unwrap();
    drop(first);
    let mut next = original.clone();
    next.runtime.packet_driver_metadata_total_max_bytes = Some(
        original
            .runtime
            .effective_packet_driver_metadata_total_max_bytes()
            + 1,
    );
    let mut prepared =
        ResourcePreparation::with_identities(&mut allocate, &identities).with_candidate(&next);
    assert_ne!(
        prepared
            .share("listener.metadata", factory, Arc::new(()))
            .unwrap(),
        id
    );
}
