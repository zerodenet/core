//! An original retry reenters network admission before reserving or borrowing.
use super::*;
type Epoch = (
    zero_core::component::NetworkGeneration,
    Option<zero_platform_tokio::contract::dns::DnsGeneration>,
);
fn stale_request(current: Epoch, requested: Epoch, retry: bool) -> bool {
    let (generation, _) = current;
    let (requested_generation, _) = requested;
    generation.0 > requested_generation.0 || retry && current != requested
}
impl Managed {
    pub(super) async fn admit_network(
        &self,
        context: &OpenContext,
        selected: Epoch,
        retry: bool,
        cleanup: Instant,
    ) -> Result<Option<tokio::sync::MutexGuard<'_, Option<Epoch>>>, ChannelError> {
        let session = self.admission.check(context)?;
        let mut network = if self.options.node() {
            Some(
                self.wait(context, Stage::Prepare, async {
                    Ok(self.network.lock().await)
                })
                .await?,
            )
        } else {
            None
        };
        let dns = matches!(
            self.options.base.endpoint.address,
            zero_core::Address::Domain(_)
        )
        .then(|| context.upstream.carrier_dns_generation())
        .flatten();
        if self.options.node()
            && (dns != selected.1
                || network.as_ref().is_some_and(|current| {
                    current.is_some_and(|old| stale_request(old, selected, retry))
                }))
        {
            // A delayed old retry cannot cancel a new epoch's live originals.
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        self.admission.check(context)?;
        if network
            .as_ref()
            .is_some_and(|current| current.is_some_and(|old| old != selected))
        {
            let records = self.records.lock().unwrap().clone();
            for record in records {
                record.stop.cancel();
            }
            self.reap(cleanup).await?;
            if !self.records.lock().unwrap().is_empty() {
                return Err(self.admission.error(
                    session,
                    Stage::Close,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ));
            }
        }
        self.reap(cleanup)
            .await
            .map_err(|source| self.admission.attach(source, session))?;
        if let Some(network) = network.as_mut() {
            **network = Some(selected);
        }
        // The caller synchronously installs its actual record under this guard,
        // then drops it before awaiting any shorter-prefix loan or H2 handshake.
        Ok(network)
    }
}
