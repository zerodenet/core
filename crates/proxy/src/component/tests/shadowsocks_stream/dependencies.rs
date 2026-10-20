use super::fixtures::*;
use crate::component::{instance::ComponentSlot, ComponentRecipe};
use shadowsocks::{
    component::{ReplayFactory, StreamFactory},
    transport::ShadowsocksTransportState,
};
use std::sync::{atomic::Ordering, Arc};
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::{component::ComponentFactory, stream::ConnectRequest, Reason};

#[tokio::test]
async fn native_2022_cannot_bypass_unstarted_or_wrong_generation_replay_dependencies() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = ShadowsocksTransportState::default();
    let assembly = assembly(&state, address, "2022-blake3-aes-128-gcm", KEY2022, 1, None);
    let error = assembly.streams[0].start(deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::Revoked);
    let replay = assembly.replay.as_ref().unwrap();
    assembly.drive(replay.start(deadline())).await.unwrap();
    let prepared = prepare(&state, address, "2022-blake3-aes-128-gcm", KEY2022, None);
    let options = prepared.bind(Some(replay.id)).unwrap();
    assert_eq!(
        StreamFactory.describe(&options).dependencies,
        vec![replay.id]
    );
    let mut wrong = replay.instance_id();
    wrong.generation = ResourceGeneration(wrong.generation.0 + 1);
    let wrong_slot = ComponentSlot::from_bound_recipe(
        assembly.streams[0].id,
        ResourceGeneration(2),
        ComponentRecipe::from_validated(Arc::new(StreamFactory), Arc::new(options)),
        Default::default(),
        "wrong_replay_generation",
        Arc::from([wrong]),
    );
    assert_eq!(
        wrong_slot.start(deadline()).await.unwrap_err().reason,
        Reason::Revoked
    );
    assert!(wrong_slot.shutdown(deadline()).await.unwrap().is_complete());
    assert!(assembly.streams[0]
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(assembly
        .drive(replay.shutdown(deadline()))
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn replay_retirement_revokes_new_native_opens_without_dial_or_unregistered_restart() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = ShadowsocksTransportState::default();
    let assembly = assembly(&state, address, "2022-blake3-aes-128-gcm", KEY2022, 1, None);
    assembly.start().await;
    let role = assembly.streams[0].stream(deadline()).await.unwrap();
    let observations = Arc::new(Observations::default());
    let report = assembly
        .drive(assembly.replay.as_ref().unwrap().shutdown(deadline()))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    let result = role
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            context(address, observations.clone()),
        )
        .await;
    let error = match result {
        Ok(_) => panic!("retired replay dependency admitted"),
        Err(error) => error,
    };
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(observations.dials.load(Ordering::Relaxed), 0);
    assert!(observations.facts.lock().unwrap().is_empty());
    assert!(assembly.streams[0]
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    // The same resource options still refer to the actual adapter state. This
    // cold description does not start an unregistered replacement driver.
    assert!(ReplayFactory
        .describe(
            &state.replay_component_options(shadowsocks::validation::ReplayPolicy::Reject, None)
        )
        .roles
        .is_empty());
}

#[tokio::test]
async fn an_id_from_another_adapter_pool_cannot_substitute_for_the_actual_security_history() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = ShadowsocksTransportState::default();
    let assembly = assembly(&state, address, "2022-blake3-aes-128-gcm", KEY2022, 1, None);
    assembly.start().await;
    let other = ShadowsocksTransportState::default();
    let options = prepare(&other, address, "2022-blake3-aes-128-gcm", KEY2022, None)
        .bind(Some(assembly.replay.as_ref().unwrap().id))
        .unwrap();
    let candidate = ComponentSlot::from_bound_recipe(
        assembly.streams[0].id,
        ResourceGeneration(2),
        ComponentRecipe::from_validated(Arc::new(StreamFactory), Arc::new(options)),
        Default::default(),
        "wrong_actual_pool",
        Arc::from([assembly.replay.as_ref().unwrap().instance_id()]),
    );
    assert_eq!(
        candidate.start(deadline()).await.unwrap_err().reason,
        Reason::Revoked
    );
    assert!(candidate.shutdown(deadline()).await.unwrap().is_complete());
    assembly.close().await;
}
