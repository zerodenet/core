use super::*;
use std::sync::Mutex;
struct Observations {
    adoption: CarrierAdoption,
    endpoints: Mutex<Option<CarrierEndpoints>>,
    panic: bool,
}
impl CarrierEndpointObserver for Observations {
    fn connected(&self, endpoints: &CarrierEndpoints) {
        assert!(endpoints.matches(&self.adoption));
        assert!(self.adoption.slot.state().view_live);
        assert!(self.adoption.endpoints().unwrap().matches(&self.adoption));
        *self.endpoints.lock().unwrap() = Some(endpoints.clone());
        if self.panic {
            panic!("actual ready callback panic");
        }
    }
}
#[tokio::test]
async fn actual_peer_and_ephemeral_local_are_same_slot_facts_with_no_total_business_deadline() {
    let mut fixture = Fixture::new(1).await;
    let grant = fixture.grants.reserve().unwrap();
    let adoption = grant.adoption();
    let observer = Arc::new(Observations {
        adoption: adoption.clone(),
        endpoints: Mutex::new(None),
        panic: false,
    });
    let prepared = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            CarrierContext {
                deadline: None,
                ..context()
            },
            grant,
        )
        .unwrap()
        .with_endpoint_observer(observer.clone());
    let carrier = prepared.open_with_deadline(None).await.unwrap();
    let (mut peer, source) = fixture.listener.accept().await.unwrap();
    assert_eq!(
        carrier.endpoints().peer(),
        fixture.listener.local_addr().unwrap()
    );
    assert_eq!(carrier.endpoints().local(), Some(source));
    assert_ne!(source.port(), 0);
    let actual = observer.endpoints.lock().unwrap().clone().unwrap();
    assert!(actual.matches(&adoption));
    assert_eq!(actual.local(), Some(source));
    let mut foreign = Fixture::new(1).await;
    let grant = foreign.grants.reserve().unwrap();
    assert!(!actual.matches(&grant.adoption()));
    drop(grant);
    assert!(foreign
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    drop(carrier);
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}
#[tokio::test]
async fn ready_observer_unwind_keeps_original_control_and_releases_only_the_logical_view() {
    let mut fixture = Fixture::new(1).await;
    let grant = fixture.grants.reserve().unwrap();
    let adoption = grant.adoption();
    let observer = Arc::new(Observations {
        adoption: adoption.clone(),
        endpoints: Mutex::new(None),
        panic: true,
    });
    let prepared = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .with_endpoint_observer(observer.clone());
    let task = tokio::spawn(prepared.open_with_deadline(None));
    let failure = match task.await {
        Err(failure) => failure,
        Ok(_) => panic!("ready callback must unwind"),
    };
    assert!(failure.is_panic());
    let (mut peer, source) = fixture.listener.accept().await.unwrap();
    assert_eq!(
        observer.endpoints.lock().unwrap().as_ref().unwrap().local(),
        Some(source)
    );
    assert!(!adoption.slot.state().view_live);
    assert!(adoption.slot.state().control.is_some());
    assert!(adoption.closed().is_none());
    assert_eq!(fixture.owner.retained(), 1);
    assert_eq!(
        peer.try_read(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}
