//! Real async command work stays in the existing original command Scope.
use super::super::ProxyHandle;
use zero_api::{ApiResult, CommandRequest, CommandResponse};
use zero_platform_tokio::contract::BoxFuture;

#[expect(
    clippy::result_large_err,
    reason = "return the original unconsumed command without a second owner"
)]
pub(super) fn prepare(
    handle: ProxyHandle,
    command: CommandRequest,
) -> Result<BoxFuture<'static, ApiResult<CommandResponse>>, CommandRequest> {
    match command {
        CommandRequest::TunStart(request) => Ok(Box::pin(async move {
            super::tun::start(&handle, &request).await
        })),
        CommandRequest::TunStop(_) => Ok(Box::pin(async move { super::tun::stop(&handle).await })),
        CommandRequest::TunRecover(_) => {
            Ok(Box::pin(async move { super::tun::recover(&handle).await }))
        }
        CommandRequest::FakeIpClear(request) => Ok(Box::pin(async move {
            super::fake_ip::clear(&handle, &request).await
        })),
        CommandRequest::DiagnosticsDnsCache(request) => Ok(Box::pin(async move {
            super::diagnostics::dns_cache(&handle, &request).await
        })),
        CommandRequest::DiagnosticsFakeipLookup(request) => Ok(Box::pin(async move {
            super::diagnostics::fakeip_lookup(&handle, &request).await
        })),
        other => super::diagnostics::asynchronous(handle, other),
    }
}
