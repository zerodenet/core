//! Cold graph checks only. No recipe below is started or used as close proof.
use super::*;
use crate::component::ComponentRecipe;
use zero_platform_tokio::contract::component::{
    BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceDeclaration,
    ResourceOwner, RoleKind,
};
struct Cold;
impl ComponentFactory for Cold {
    type Options = ComponentSpec;
    fn describe(&self, spec: &ComponentSpec) -> ComponentSpec {
        spec.clone()
    }
    fn build(
        &self,
        _: Arc<ComponentSpec>,
        _: BuildContext,
    ) -> Result<ComponentInstance, ChannelError> {
        Err(invalid(
            Reason::Unsupported,
            "cold graph fixture cannot execute",
        ))
    }
}
fn recipe(deps: &[u64], private: bool, endpoint: bool) -> ComponentRecipe {
    ComponentRecipe::from_validated(
        Arc::new(Cold),
        Arc::new(ComponentSpec {
            owner: if endpoint {
                ResourceOwner::Endpoint("shared-dns".into())
            } else {
                ResourceOwner::Component
            },
            roles: if private {
                vec![]
            } else {
                vec![RoleKind::Stream]
            },
            dependencies: deps.iter().copied().map(ComponentId).collect(),
            static_identity: Arc::from([]),
            task_limit: None,
            resources: if private {
                vec![ResourceDeclaration::ControlledCarrierPool {
                    endpoint: zero_core::request::Destination::new(
                        zero_core::Address::Ipv4([127, 0, 0, 1]),
                        443,
                    ),
                    dial: Default::default(),
                    max_sockets: std::num::NonZeroUsize::new(1).unwrap(),
                    datagram: false,
                }]
            } else {
                vec![]
            },
        }),
    )
}
fn config(groups: &str) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"a","protocol":{{"type":"direct"}}}},{{"tag":"b","protocol":{{"type":"direct"}}}}],"outbound_groups":{groups},"route":{{"final":{{"type":"direct"}}}}}}"#)).unwrap()
}
fn named() -> BTreeMap<String, ComponentId> {
    BTreeMap::from([("a".into(), ComponentId(1)), ("b".into(), ComponentId(2))])
}

#[test]
fn private_lower_and_upper_share_a_lifecycle_but_shared_dns_and_acyclic_donors_do_not() {
    let nodes = BTreeMap::from([
        (ComponentId(1), recipe(&[3], false, false)),
        (ComponentId(2), recipe(&[], false, false)),
        (ComponentId(3), recipe(&[4], true, false)),
        (ComponentId(4), recipe(&[], true, true)),
    ]);
    let result = owners(
        nodes.iter().map(|(id, recipe)| (*id, recipe)),
        &named(),
        &config(r#"[{"tag":"relay","type":"relay","proxies":["b","a"]}]"#),
    )
    .unwrap();
    assert_eq!(result[&ComponentId(1)], result[&ComponentId(3)]);
    assert_ne!(result[&ComponentId(1)], result[&ComponentId(2)]);
    assert!(!result.contains_key(&ComponentId(4)));
}

#[test]
fn opposite_existing_prefixes_couple_all_actual_pool_and_lower_members() {
    let nodes = BTreeMap::from([
        (ComponentId(1), recipe(&[3], false, false)),
        (ComponentId(2), recipe(&[4], false, false)),
        (ComponentId(3), recipe(&[], true, false)),
        (ComponentId(4), recipe(&[], true, false)),
    ]);
    let result = owners(nodes.iter().map(|(id, recipe)| (*id, recipe)), &named(),
        &config(r#"[{"tag":"ab","type":"relay","proxies":["a","b"]},{"tag":"ba","type":"relay","proxies":["b","a"]}]"#)).unwrap();
    assert_eq!(result.len(), 4);
    assert!(result.values().all(|owner| *owner == ComponentId(1)));
}

#[test]
fn device_packet_owners_are_not_absorbed_into_a_stream_pool_group() {
    let mut device = recipe(&[3], false, false).spec().clone();
    device.roles = vec![RoleKind::Packet];
    let nodes = BTreeMap::from([
        (
            ComponentId(1),
            ComponentRecipe::from_validated(Arc::new(Cold), Arc::new(device)),
        ),
        (ComponentId(2), recipe(&[], false, false)),
        (ComponentId(3), recipe(&[], true, false)),
    ]);
    let result = owners(
        nodes.iter().map(|(id, recipe)| (*id, recipe)),
        &named(),
        &config("[]"),
    )
    .unwrap();
    assert!(!result.contains_key(&ComponentId(1)));
    assert!(!result.contains_key(&ComponentId(3)));
}
