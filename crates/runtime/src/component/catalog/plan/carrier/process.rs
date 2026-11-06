//! Cold shape only; actual IO still requires the matching started owner/grant.
use zero_platform_tokio::contract::component::ComponentSpec;
pub(super) fn paired_endpoint(spec: &ComponentSpec) -> bool {
    crate::component::instance::demand_process_endpoint_ordinal(spec).is_some()
}
