//! Same actual Runtime Host source allocation through native close and ACK.
use super::*;
#[tokio::test]
async fn actual_host_metadata_sources_hold_the_same_root_until_the_last_getter_returns() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let fixture = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let roots = fixture.slot.bound_original_budgets().unwrap();
    let memory = fixture.resources.packet_driver_metadata().unwrap();
    assert!(memory.same_root(&roots.packet.driver_metadata_view()));
    let before = memory.snapshot().occupied;
    let (query, release) = fixture.query();
    let permit = fixture.grants.reserve(&query).unwrap();
    let original = permit.origin();
    let sources = original.original_sources().unwrap();
    let registration = original.registration_observation();
    assert!(memory.snapshot().occupied.bytes() > before.bytes());
    let grant = open(&permit, &query);
    let returned = grant
        .return_unstarted()
        .unwrap_or_else(|_| panic!("same original users returned"));
    let users = returned.try_closed().unwrap();
    let closed = permit.close(Some(&users), &fixture.tasks).unwrap();
    assert!(closed.matches(&original) && closed.had_native_acquisition());
    assert!(
        registration.released().is_none(),
        "native close is not the original registration return"
    );
    assert_eq!(fixture.grants.retained(), Some(1));
    assert!(fixture.grants.reserve(&query).is_err());
    fixture.complete_query(&query, release).await;
    assert!(permit
        .close(Some(&users), &fixture.tasks)
        .unwrap()
        .matches(&original));
    assert_eq!(fixture.grants.retained(), Some(0));
    let released = registration
        .released()
        .expect("same original registration actually returned");
    assert!(released.matches(&registration));
    let (fresh_query, fresh_release) = fixture.query();
    let fresh = fixture.grants.reserve(&fresh_query).unwrap();
    let fresh_registration = fresh.origin().registration_observation();
    assert!(
        !released.matches(&fresh_registration),
        "same Scope and ordinal namespace cannot substitute a fresh original"
    );
    assert!(!fresh
        .close(None, &fixture.tasks)
        .unwrap()
        .had_native_acquisition());
    fixture.complete_query(&fresh_query, fresh_release).await;
    drop(fresh);
    drop(fresh_registration);
    drop(released);
    drop(registration);
    drop(closed);
    drop(original);
    drop(permit);
    drop(returned);
    drop(users);
    assert!(
        memory.snapshot().occupied.bytes() > before.bytes(),
        "the real external source getter still owns its actual allocation"
    );
    drop(sources);
    assert_eq!(memory.snapshot().occupied, before);
    assert!(fixture.finish().await.is_complete());
}
