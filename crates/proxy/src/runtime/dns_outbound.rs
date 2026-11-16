//! Native DNS regression sources. Proxy execution uses the actual host's
//! Query/Node providers directly, without a proxy-local connector or executor.
#[path = "dns_outbound/tests.rs"]
mod tests;
