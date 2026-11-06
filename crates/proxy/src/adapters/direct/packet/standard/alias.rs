use super::*;
use zero_platform_tokio::contract::component::BuildContext;
#[derive(Clone)]
pub(in crate::adapters::direct) struct Alias {
    pub(super) root: Arc<Options>,
    pub(super) id: ComponentId,
}
impl std::fmt::Debug for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectPacketAlias")
            .field("component", &self.id)
            .finish_non_exhaustive()
    }
}
impl PartialEq for Alias {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.root.spec == other.root.spec
    }
}
impl Eq for Alias {}
impl Alias {
    pub(in crate::adapters::direct) fn dependency(&self) -> ComponentId {
        self.id
    }
    pub(in crate::adapters::direct) fn identity(&self) -> &[u8] {
        &self.root.spec.static_identity
    }
    pub(in crate::adapters::direct) fn role(
        &self,
        context: &BuildContext,
    ) -> Result<Arc<dyn DynPacketOutbound>, ChannelError> {
        if context.dependency(self.id).is_none() {
            return Err(error(
                Reason::StalePlan,
                "Direct Packet alias has no actual native dependency",
            ));
        }
        self.root
            .role
            .get()
            .and_then(Weak::upgrade)
            .map(|role| role as Arc<dyn DynPacketOutbound>)
            .ok_or_else(|| error(Reason::Revoked, "same Direct Packet root role was released"))
    }
}
