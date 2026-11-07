//! The existing H2 stream task retains its concrete request and wire owners.
mod command;
mod config_query;
mod executor;
mod input;
pub(super) mod output;
mod registry;
mod runtime_query;
pub(super) use executor::StreamExecutor;
pub(super) use input::Input;
pub(super) use output::{Output, Payload};
pub(super) use registry::{Record, Requests};
// Pinned tonic 0.12.3's ProstCodec default geometry (codec/mod.rs), not
// production quotas. The generated service selects that same default codec.
const CODEC_BUFFER_SIZE: usize = 8 * 1024;
const CODEC_YIELD_THRESHOLD: usize = 32 * 1024;
tokio::task_local! {
    static CURRENT: std::sync::Arc<Record>;
}
pub(super) fn current() -> Option<std::sync::Arc<Record>> {
    CURRENT.try_with(Clone::clone).ok()
}
pub(super) fn request<T>(
    request: &tonic::Request<T>,
) -> Result<Option<std::sync::Arc<Record>>, tonic::Status> {
    if let Some(actual) = request.extensions().get::<std::sync::Arc<Record>>() {
        return Ok(Some(actual.clone()));
    }
    // Direct private handler fixtures have no HTTP/2 executor. Production
    // requests must always carry the original accepted stream registration.
    #[cfg(test)]
    return Ok(None);
    #[cfg(not(test))]
    Err(tonic::Status::internal(
        "original accepted gRPC request absent",
    ))
}
#[cfg(test)]
#[path = "../tests/encoded_bytes.rs"]
mod tests;

mod initial_snapshot;
#[cfg(feature = "proxy-control")]
mod proxy_query;
mod traffic_query;

#[cfg(feature = "proxy-control")]
mod endpoint_query;
