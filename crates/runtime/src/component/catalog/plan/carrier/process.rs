//! Cold dependency shape only; it does not authorize process or loopback IO.
use zero_platform_tokio::contract::component::{ComponentSpec, ResourceDeclaration};
pub(super) fn paired_endpoint(spec: &ComponentSpec) -> bool {
    spec.roles.is_empty()
        && spec.dependencies.is_empty()
        && spec.task_limit.is_some_and(|limit| limit.get() == 1)
        && matches!(spec.resources.as_slice(), [
            ResourceDeclaration::Process { max_starts, .. },
            ResourceDeclaration::ProcessEndpoint { process_ordinal: 0, .. }
        ] if max_starts.get() == 1)
}
