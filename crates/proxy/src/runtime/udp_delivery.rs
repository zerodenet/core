//! UDP response writer execution with neutral traffic accounting.

use std::future::Future;

mod helpers;

pub(crate) use helpers::*;

#[cfg(feature = "udp-response-runtime")]
pub(crate) async fn write_direct_response<F, Fut, E>(
    response: &UdpDirectResponseParts<'_>,
    write: F,
) -> Result<usize, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<usize, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(0);
    }
    let written = write().await?;
    response.accounting.record_sent(written);
    Ok(written)
}

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) async fn write_optional_direct_response<F, Fut, E>(
    response: &UdpDirectResponseParts<'_>,
    write: F,
) -> Result<Option<usize>, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<usize>, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(None);
    }
    let written = write().await?;
    if let Some(written) = written {
        response.accounting.record_sent(written);
    }
    Ok(written)
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) async fn write_upstream_response<F, Fut, E>(
    response: &UdpUpstreamResponseParts,
    write: F,
) -> Result<usize, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<usize, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(0);
    }
    let written = write().await?;
    response.accounting.record_sent(written);
    Ok(written)
}

#[cfg(all(
    feature = "upstream-association-runtime",
    any(
        feature = "managed-datagram-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) async fn write_optional_upstream_response<F, Fut, E>(
    response: &UdpUpstreamResponseParts,
    write: F,
) -> Result<Option<usize>, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<usize>, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(None);
    }
    let written = write().await?;
    if let Some(written) = written {
        response.accounting.record_sent(written);
    }
    Ok(written)
}

#[cfg(feature = "udp-response-runtime")]
pub(crate) async fn write_chain_response<F, Fut, E>(
    response: &UdpChainResponseParts,
    write: F,
) -> Result<usize, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<usize, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(0);
    }
    let written = write().await?;
    response.accounting.record_sent(written);
    Ok(written)
}

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) async fn write_optional_chain_response<F, Fut, E>(
    response: &UdpChainResponseParts,
    write: F,
) -> Result<Option<usize>, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<usize>, E>>,
{
    if !response
        .accounting
        .throttle_download(response.payload.len())
        .await
    {
        return Ok(None);
    }
    let written = write().await?;
    if let Some(written) = written {
        response.accounting.record_sent(written);
    }
    Ok(written)
}
