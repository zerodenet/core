use super::*;
use zero_platform_tokio::{
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
    controlled_datagram::metadata_pool::{MetadataPoolOrigin, MetadataPoolRegistration},
};
pub(in crate::adapters::vless::component::tests::material) enum Original {
    Native(MetadataPoolRegistration, MetadataPoolOrigin),
    Borrowed(CarrierOwnerRegistration, CarrierOriginObservation),
}
impl Original {
    pub async fn capture(client: &Running, borrowed: bool) -> Self {
        let plan = client
            .proxy
            .tcp_runtime_services()
            .components
            .clone()
            .unwrap();
        let slot = plan.configured("node").unwrap();
        if !borrowed {
            let owner = slot.original_metadata_owner(0).unwrap();
            let origin = owner.current_original().expect("actual main UDP original");
            assert_eq!(owner.limit().get(), 1);
            let caller = origin
                .initializer_registration()
                .expect("actual establishing Aux");
            let joined = caller.wait_registration_released().await;
            assert!(caller.matches_joined(&joined) && joined.registration_released());
            assert!(origin.closed().is_none());
            assert!(origin.registration_observation().released().is_none());
            Self::Native(owner, origin)
        } else {
            let owner = slot.original_carrier_owner(1).unwrap();
            let observed = owner.observe().unwrap();
            assert_eq!(observed.retained(), 1);
            let record = &observed.slots()[0];
            assert!(record.native().is_none());
            assert!(record.borrowed_origin().is_some());
            let caller = record.auxiliary().expect("actual original setup").clone();
            let joined = caller.wait_registration_released().await;
            assert!(caller.matches_joined(&joined) && joined.registration_released());
            assert!(!record.driver().expect("actual packet driver").1.is_joined());
            Self::Borrowed(owner, record.original().clone())
        }
    }
    pub fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Native(_, a), Self::Native(_, b)) => a.same_original(b),
            (Self::Borrowed(_, a), Self::Borrowed(_, b)) => a.same_original(b),
            _ => false,
        }
    }
    pub async fn returned(&self) {
        timeout(Duration::from_secs(3), async {
            loop {
                let returned = match self {
                    Self::Native(_, origin) => {
                        origin.closed().is_some()
                            && origin.registration_observation().released().is_some()
                    }
                    Self::Borrowed(owner, origin) => {
                        origin.closed().is_some()
                            && owner
                                .observe()
                                .is_some_and(|current| current.retained() == 0)
                    }
                };
                if returned {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match self {
            Self::Native(_, origin) => {
                let closed = origin.closed().unwrap();
                assert!(closed.matches(origin) && closed.had_native_acquisition());
                let observed = origin.registration_observation();
                assert!(observed.released().unwrap().matches(&observed));
            }
            Self::Borrowed(_, origin) => {
                let closed = origin.closed().unwrap();
                assert!(origin.matches_closed(&closed) && !closed.had_native_acquisition());
            }
        }
    }
    pub fn closed_owner(&self) {
        match self {
            Self::Native(owner, _) => assert!(owner.closed().unwrap().matches(owner)),
            Self::Borrowed(owner, _) => {
                assert!(owner.closed().unwrap().matches(owner));
                assert!(owner.users_closed().unwrap().matches(owner));
            }
        }
    }
}
