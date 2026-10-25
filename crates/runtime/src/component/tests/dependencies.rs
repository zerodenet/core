use crate::component::{
    catalog::ComponentCatalog, dependencies::order, DirectFactory, DirectOptions,
};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{
    component::{ComponentFactory, ComponentSpec},
    DeliveryState, Impact, Reason, Stage,
};

fn spec(dependencies: Vec<ComponentId>) -> ComponentSpec {
    let options = DirectOptions::validate(Default::default()).unwrap();
    let mut spec = DirectFactory.describe(&options);
    spec.dependencies = dependencies;
    spec
}

#[test]
fn shared_prerequisites_have_one_position_before_each_consumer() {
    let ids = ComponentCatalog::default();
    let shared = ids.allocate_id().unwrap();
    let left = ids.allocate_id().unwrap();
    let right = ids.allocate_id().unwrap();
    let final_node = ids.allocate_id().unwrap();
    let nodes = [
        (final_node, spec(vec![left, right])),
        (right, spec(vec![shared])),
        (shared, spec(vec![])),
        (left, spec(vec![shared])),
    ];
    let sequence = order(nodes.iter().map(|(id, spec)| (*id, spec))).unwrap();
    assert_eq!(sequence, [shared, left, right, final_node]);
}

#[test]
fn unknown_dependencies_fail_with_the_actual_consumer_identity_before_build() {
    let ids = ComponentCatalog::default();
    let consumer = ids.allocate_id().unwrap();
    let absent = ids.allocate_id().unwrap();
    let options = spec(vec![absent]);
    let error = order([(consumer, &options)]).unwrap_err();
    assert_eq!(error.component, Some(consumer));
    assert_eq!(error.stage, Stage::Prepare);
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert_eq!(error.impact, Impact::Component);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains(&absent.0.to_string()));
}

#[test]
fn duplicate_component_identities_cannot_replace_an_earlier_node() {
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let options = spec(vec![]);
    let error = order([(id, &options), (id, &options)]).unwrap_err();
    assert_eq!(error.component, Some(id));
    assert!(message(&error).contains("duplicate component identity"));
}

#[test]
fn duplicate_dependency_entries_are_rejected() {
    let ids = ComponentCatalog::default();
    let root = ids.allocate_id().unwrap();
    let consumer = ids.allocate_id().unwrap();
    let root_spec = spec(vec![]);
    let consumer_spec = spec(vec![root, root]);
    let error = order([(root, &root_spec), (consumer, &consumer_spec)]).unwrap_err();
    assert_eq!(error.component, Some(consumer));
    assert!(message(&error).contains("duplicate component dependency"));
}

#[test]
fn self_and_indirect_dependency_cycles_are_rejected_without_starting_a_runtime() {
    let ids = ComponentCatalog::default();
    let first = ids.allocate_id().unwrap();
    let second = ids.allocate_id().unwrap();
    let self_cycle = spec(vec![first]);
    assert!(message(&order([(first, &self_cycle)]).unwrap_err()).contains("cycle"));
    let first_spec = spec(vec![second]);
    let second_spec = spec(vec![first]);
    let error = order([(second, &second_spec), (first, &first_spec)]).unwrap_err();
    assert_eq!(error.component, Some(first));
    assert!(message(&error).contains("cycle"));
}

#[test]
fn deep_dependency_chains_use_no_recursive_validation_stack() {
    let ids = ComponentCatalog::default();
    let mut nodes = Vec::new();
    let mut previous = None;
    for _ in 0..10_000 {
        let id = ids.allocate_id().unwrap();
        nodes.push((id, spec(previous.into_iter().collect())));
        previous = Some(id);
    }
    let expected: Vec<_> = nodes.iter().map(|(id, _)| *id).collect();
    let sequence = order(nodes.iter().rev().map(|(id, spec)| (*id, spec))).unwrap();
    assert_eq!(sequence, expected);
}

fn message(error: &zero_platform_tokio::contract::ChannelError) -> String {
    std::error::Error::source(error).unwrap().to_string()
}
