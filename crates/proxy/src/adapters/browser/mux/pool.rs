//! Same retained MUX pool, with a declared real Browser carrier dependency.
use crate::component::background::{pool, BackgroundCarrier};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::*, lifecycle::BuildError, ChannelError, Impact, Reason, Stage,
};
pub(crate) struct Factory {
    pub background: Arc<pool::Factory>,
    pub dependency: zero_core::component::ComponentId,
    pub source: zero_transport::browser_dialer::component::raw::RawCarrier,
}
impl ComponentFactory for Factory {
    type Options = pool::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        self.background.describe(options)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let expected = context
            .dependency(self.dependency)
            .filter(|actual| *actual != context.instance())
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
            })?;
        let provider = Arc::new(BackgroundCarrier {
            network: self.background.network.clone(),
            instance: context.instance(),
            initial: options.pool.binding().clone(),
            endpoint: options.pool.endpoint().clone(),
            wire_endpoint: options.wire_endpoint.clone(),
            tag: options.tag.clone(),
        });
        context.background_carrier = Some(super::lender::transport(
            provider,
            options.pool.binding().clone(),
            expected,
            self.source.clone(),
        ));
        zero_transport::pooled_driver::PoolFactory.build(
            Arc::new((*options.pool).clone().with_scoped_preparation()),
            context,
        )
    }
}
