use alloc::vec::Vec;
use zero_core::Error;
use zero_traits::AsyncSocket;

/// Rvs omits the destination address and port, just like Mux.Cool. It is only
/// emitted by an explicitly configured reverse worker, never by domain routing.
pub async fn send_request<S: AsyncSocket>(
    stream: &mut S,
    uuid: &[u8; 16],
    flow: Option<&str>,
) -> Result<usize, Error> {
    #[cfg(feature = "reality")]
    let addons = crate::flow::encode_addons(flow)?;
    #[cfg(not(feature = "reality"))]
    let addons = {
        if flow.is_some() {
            return Err(Error::Unsupported("Rvs flow requires the reality feature"));
        }
        alloc::vec![0]
    };
    let mut request = Vec::with_capacity(18 + addons.len());
    request.push(crate::VLESS_VERSION);
    request.extend_from_slice(uuid);
    request.extend_from_slice(&addons);
    request.push(super::COMMAND);
    stream
        .write_all(&request)
        .await
        .map_err(|_| Error::Io("failed to write Rvs request"))?;
    Ok(request.len())
}
