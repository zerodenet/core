//! Explicit host-fence fixture using real Engine admission and raw caller Scope.
use super::*;
use crate::component::business_demand::AdmittedBusinessDemand;
use zero_core::{
    component::{OutboundId, RoleId, RoleInstanceId, SessionId},
    Network, ProtocolType, Session,
};
use zero_platform_tokio::{
    contract::CancellationToken,
    controlled_carrier::{
        CarrierAdoption, CarrierBusinessDemand, CarrierGrant, CarrierIo, CarrierOwnerBinding,
        CarrierUserView,
    },
};
pub(super) struct Receiver {
    pub owner: CarrierOwner,
    pub scope: TaskScope,
    pub original: CarrierAdoption,
    setup: Option<Setup>,
}
pub(super) struct Setup {
    grant: CarrierGrant,
    view: CarrierUserView,
    bound: CarrierOwnerBinding,
    fence: Arc<CancellationToken>,
    binding: UpstreamBinding,
}
impl Receiver {
    pub async fn new(
        endpoint: Destination,
        donor: ComponentInstanceId,
        generation: NetworkGeneration,
    ) -> Self {
        let cap = NonZeroUsize::new(1).unwrap();
        let scope = TaskScope::new(tokio::runtime::Handle::current(), cap);
        let fence = Arc::new(CancellationToken::new());
        let instance = ComponentInstanceId {
            component: ComponentId(975),
            generation: ResourceGeneration(1),
        };
        let mut owner = CarrierOwner::new(instance, cap, &fence);
        let bound = owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledNodeStream {
                    endpoint,
                    dial: Default::default(),
                    max_sockets: cap,
                },
                &scope.registrar(),
                &fence,
            )
            .unwrap();
        owner
            .start(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_user_view().unwrap();
        let original = grant.adoption();
        // The explicit library host assigns this role to its real donor instance;
        // it is not a new Scope task or authority to bypass the host fence.
        let binding = UpstreamBinding {
            prefix: vec![RoleInstanceId {
                component: donor.component,
                role: RoleId::Outbound(OutboundId(974)),
                generation: donor.generation,
            }],
            dial: Default::default(),
            network_generation: generation,
        };
        Self {
            owner,
            scope,
            original,
            setup: Some(Setup {
                grant,
                view,
                bound,
                fence,
                binding,
            }),
        }
    }
    pub fn take(&mut self) -> Setup {
        self.setup.take().unwrap()
    }
}
struct Demand {
    engine: zero_engine::Engine,
    admitted: AdmittedBusinessDemand,
    caller: NativeRegistration,
    session: SessionId,
    deadline: Instant,
}
impl CarrierBusinessDemand for Demand {
    fn check(&self) -> Result<(), ChannelError> {
        self.caller.check_open()?;
        self.admitted
            .check_live(&self.engine, self.session)
            .map_err(|source| rejected(Reason::Revoked).with_source(source))
    }
    fn session(&self) -> SessionId {
        self.session
    }
    fn cancellation(&self) -> CancellationToken {
        self.admitted.cancellation()
    }
    fn deadline(&self) -> Option<Instant> {
        Some(self.deadline)
    }
}
impl Setup {
    pub fn adopt(
        self,
        engine: zero_engine::Engine,
        caller: NativeRegistration,
        endpoint: Destination,
        io: CarrierIo,
        view: CarrierUserView,
    ) -> Result<(CarrierIo, CarrierUserView), zero_engine::EngineError> {
        let mut session = Session::new(
            0,
            endpoint.address.clone(),
            endpoint.port,
            Network::Tcp,
            ProtocolType::new("raw-pending"),
        );
        let receipt = engine.prepare_session_admitted(&mut session, "raw-pending")?;
        let admitted = AdmittedBusinessDemand::new(&engine, SessionId(session.id), receipt)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let demand = Arc::new(Demand {
            engine,
            admitted,
            caller: caller.clone(),
            session: SessionId(session.id),
            deadline,
        });
        let issuer = self
            .bound
            .node_relay_business_issuer(&self.fence, &self.grant, &endpoint, &self.binding)
            .map_err(std::io::Error::other)?;
        let ticket = issuer
            .authorize_with_caller(demand.clone(), caller.caller_witness().unwrap())
            .map_err(std::io::Error::other)?;
        let original = self.grant.adoption();
        caller
            .observe_pending_carrier(original.clone())
            .map_err(std::io::Error::other)?;
        let owner = original
            .cleanup_owner()
            .expect("same actual reserved owner");
        let loan = io.into_stream_loan(view).map_err(std::io::Error::other)?;
        let io = self
            .grant
            .adopt_business_stream_loan(
                ticket,
                &self.binding,
                endpoint,
                &CarrierContext {
                    session: Some(demand.session),
                    deadline: Some(deadline),
                    cancel: demand.cancellation(),
                },
                loan,
            )
            .map_err(std::io::Error::other)?;
        caller
            .observe_carrier_with_owner(&owner, original)
            .map_err(std::io::Error::other)?;
        Ok((io, self.view))
    }
}
