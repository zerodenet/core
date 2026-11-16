//! The existing UDP role without an unrelated TCP or SIP003 process owner.
use super::{role, UdpPoolFactory, UdpPoolOptions};
use std::sync::Arc;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    datagram::erase_datagram,
    lifecycle::{erase_lifecycle, BuildError},
    ChannelError, Impact, Reason, Stage,
};

pub struct DatagramOptions {
    role: role::RoleOptions,
    identity: Arc<[u8]>,
}
impl UdpPoolOptions {
    pub fn bind_datagram(&self, pool: ComponentId) -> DatagramOptions {
        let mut identity = UdpPoolFactory.describe(self).static_identity.to_vec();
        identity.extend_from_slice(b"same-ss-datagram-role-v1");
        identity.extend_from_slice(&pool.0.to_le_bytes());
        DatagramOptions {
            role: self.role(pool),
            identity: identity.into(),
        }
    }
}
pub struct DatagramFactory;
impl ComponentFactory for DatagramFactory {
    type Options = DatagramOptions;
    fn describe(&self, options: &DatagramOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            static_identity: options.identity.clone(),
            roles: vec![RoleKind::Datagram],
            dependencies: std::iter::once(options.role.pool)
                .chain(options.role.process.map(|(id, _)| id))
                .collect(),
            resources: vec![options.role.declaration()],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<DatagramOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let expected = self.describe(&options).dependencies;
        if context.dependencies.len() != expected.len()
            || expected.iter().any(|id| context.dependency(*id).is_none())
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let (owner, outbound) = role::RoleOwner::build(options.role.clone(), &context, 0)?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                datagram: Some(erase_datagram(outbound)),
                ..Default::default()
            },
        })
    }
}
