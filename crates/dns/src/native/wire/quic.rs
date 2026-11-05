//! DNS framing remains here; the executable provider owns Quinn and originals.
use super::*;
use tokio::io::AsyncWriteExt;
pub(super) async fn exchange(
    provider: &dyn NativeDnsProvider,
    backend: &crate::backends::ResolverBackend,
    request: &Query<'_>,
    peer: SocketAddr,
) -> Attempt {
    let (name, client) = match backend.native_quic_profile() {
        Ok(profile) => profile,
        Err(source) => return Attempt::blocked(source),
    };
    let opening = provider.quic(
        peer,
        name,
        client,
        request.caller.clone(),
        request.cancel.clone(),
        request.end,
    );
    let loan = match OwnedDnsQueryTasks::native_io(opening).await {
        Ok(Ok(loan)) => loan,
        Ok(Err(source)) | Err(source) => return Attempt::open_failed(source),
    };
    let crate::native::NativeDnsQuicLoan { mut io, mut owner } = loan;
    if let Err(source) = OwnedDnsQueryTasks::record_metadata(owner.original()) {
        drop(io);
        return attempt::settle_quic(owner.as_mut(), request.end, Err(source)).await;
    }
    let work = async {
        let length: u16 =
            request.bytes.len().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "DNS message is too large")
            })?;
        io.write_u16(length).await?;
        io.write_all(request.bytes).await?;
        io.shutdown().await?;
        crate::backends::read_framed(&mut io).await
    };
    let response = attempt(request.end, async {
        OwnedDnsQueryTasks::native_io(work).await?
    })
    .await;
    drop(io);
    attempt::settle_quic(owner.as_mut(), request.end, response).await
}
