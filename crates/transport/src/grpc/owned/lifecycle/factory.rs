//! Same factory construction, with the existing runtime ECH resolver injected.
use super::*;
impl NativePoolFactory {
    #[cfg(feature = "tls")]
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<NativePoolOptions>,
        context: BuildContext,
        resolver: Arc<dyn crate::tls::ech::EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    pub(super) fn build_owned(
        &self,
        options: Arc<NativePoolOptions>,
        context: BuildContext,
        #[cfg(feature = "tls")] ech_resolver: Option<Arc<dyn crate::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        #[cfg(feature = "tls")]
        if super::ech::dynamic(&options).map_err(|source| {
            error(instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
        })? && (!options.selected_background
            || options.node_dns.is_none()
            || ech_resolver.is_none())
        {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        if context.dependencies.len()
            != usize::from(options.totals.is_some()) + usize::from(options.node_dns.is_some())
            || options
                .node_dns
                .is_some_and(|node| context.dependency(node).is_none())
            || options.node_dns.is_some_and(|node| {
                options
                    .totals
                    .as_ref()
                    .is_some_and(|root| root.dependency == node)
            })
            || options
                .totals
                .as_ref()
                .is_some_and(|totals| context.dependency(totals.dependency).is_none())
            || context.roles != Default::default()
        {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        if options
            .settings
            .tls
            .as_ref()
            .is_some_and(|tls| tls.ca_cert_path.is_some())
            != options.ca.is_some()
        {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        if options.selected_background
            && matches!(options.endpoint.address, zero_core::Address::Domain(_))
            && options.node_dns.is_none()
        {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        let upstream = context
            .background_carrier
            .clone()
            .filter(|upstream| {
                upstream.binding().prefix.is_empty()
                    && upstream.binding().dial == options.settings.dial
            })
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::InvalidConfig))?;
        let pool = options
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        let cancel = context.cancel.child_token();
        let carriers = CarrierOwner::new(instance, options.carrier_limit, &cancel);
        let managed = Managed::new(
            instance,
            options,
            upstream,
            context.tasks,
            cancel,
            carriers.grants(),
            #[cfg(feature = "tls")]
            ech_resolver,
        );
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                pool,
                managed,
                carriers,
                resources: context.resources,
                attempted: false,
                claimed: false,
            }),
        })
    }
}
