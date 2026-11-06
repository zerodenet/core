//! Host-selected process dependency, independent of physical CarrierOrigin.
use super::*;
use crate::owned_process::ProcessEndpointUse;
impl NativeCarrierOpener {
    pub fn business_process_endpoint(
        grant: super::super::CarrierBusinessGrant,
        derived: ProcessEndpointUse,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let instance = grant.binding().registration().instance();
        let fail = |source| {
            error(
                instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        };
        let derived = derived.with_egress(egress.clone()).map_err(fail)?;
        let peer = derived.peer().map_err(fail)?;
        Ok(Self {
            grantee: instance,
            related_parent: None,
            derived: Some(derived),
            binding: grant.upstream().clone(),
            peer,
            endpoint: grant.endpoint().clone(),
            egress,
            observer,
            business: Some(grant),
            service: None,
            dns: None,
        })
    }
    pub fn service_process_endpoint(
        grant: super::super::CarrierServiceGrant,
        derived: ProcessEndpointUse,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let instance = grant.binding().registration().instance();
        let fail = |source| {
            error(
                instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        };
        let derived = derived.with_egress(egress.clone()).map_err(fail)?;
        let peer = derived.peer().map_err(fail)?;
        Ok(Self {
            grantee: instance,
            related_parent: None,
            derived: Some(derived),
            binding: grant.upstream().clone(),
            peer,
            endpoint: grant.endpoint().clone(),
            egress,
            observer,
            business: None,
            service: Some(grant),
            dns: None,
        })
    }
    /// The existing actual background Stream/Datagram owner supplies its own finite native grant
    /// and background caller custody. This does not create a Business session or
    /// broaden packet()'s existing session=None constraint.
    pub fn background_process_endpoint(
        grant: &CarrierGrant,
        binding: UpstreamBinding,
        endpoint: Destination,
        derived: ProcessEndpointUse,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        let instance = grant.instance();
        let fail = |source| {
            error(
                instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        };
        let derived = derived.with_egress(egress.clone()).map_err(fail)?;
        let peer = derived.peer().map_err(fail)?;
        Ok(Self {
            grantee: instance,
            related_parent: None,
            derived: Some(derived),
            binding,
            peer,
            endpoint,
            egress,
            observer,
            business: None,
            service: None,
            dns: None,
        })
    }
}
