//! Fresh original after a genuine socket-connect failure, never protocol replay.
use super::*;
impl Outbound {
    pub(super) fn context_for_attempt(
        &self,
        context: &OpenContext,
        transfer: &Arc<Transfer>,
        caller: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    ) -> Result<OpenContext, ChannelError> {
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(transfer.adoption.clone())?;
        }
        let mut next = context.clone();
        let pending = caller
            .map(|caller| super::pending::transport(context, transfer, caller))
            .transpose()?;
        if let (Some(preparation), Some(pending)) = (&transfer.preparation, &pending) {
            preparation.bind_pending_observer(pending.pending_carrier_observer())?;
        }
        *transfer.preparing.lock().unwrap() = pending;
        next.upstream = UpstreamTransport::new(
            context.upstream.binding().clone(),
            Arc::new(Opener {
                upstream: context.upstream.clone(),
                endpoint: self.endpoint.clone(),
                process: self.process,
                transfer: transfer.clone(),
                association: None,
            }),
        );
        Ok(next)
    }
    pub(super) async fn retry_original(
        &self,
        context: &OpenContext,
        transfer: &Arc<Transfer>,
        source: &ChannelError,
    ) -> Result<bool, ChannelError> {
        #[cfg(test)]
        eprintln!("actual original retry input endpoint={:?} prefixEmpty={} nativeConnected={} taggedConnect={:?} cancelled={} expired={} originalClosed={:?}",
            self.endpoint, context.upstream.binding().prefix.is_empty(), transfer.adoption.endpoints().is_some(),
            zero_platform_tokio::controlled_carrier::original_connect_error(source).map(|original| original.stage()),
            context.cancel.is_cancelled(), context.deadline.is_some_and(|end| Instant::now() >= end),
            transfer.adoption.closed().map(|native| (native.had_native_acquisition(),native.borrowed_origin().is_some())));
        if !matches!(self.endpoint.address, zero_core::Address::Domain(_))
            || !context.upstream.binding().prefix.is_empty()
            || !zero_platform_tokio::controlled_carrier::original_connect_error(source)
                .is_some_and(|original| original.stage() == "connect_socket")
            || transfer.adoption.endpoints().is_some()
            || context.cancel.is_cancelled()
            || context.deadline.is_some_and(|end| Instant::now() >= end)
        {
            return Ok(false);
        }
        // The same upstream retains its whole-demand Node candidate cursor.
        // Never advance until this original's view, native, actual Aux and ACK
        // have really settled. No new protocol/TLS handshake was submitted.
        let report = match context.deadline {
            Some(end) => transfer.close(end).await?,
            None => transfer.try_close()?,
        };
        #[cfg(test)]
        eprintln!("actual original retry settled report={report:?} finished={} ownClosed={:?}", transfer.finished(),
            transfer.adoption.closed().map(|native| (native.had_native_acquisition(),native.borrowed_origin().is_some())));
        Ok(report.is_complete()
            && transfer.finished()
            && transfer.adoption.closed().is_some_and(|native| {
                native.had_native_acquisition() && native.borrowed_origin().is_none()
            }))
    }
}
