use crate::runtime::tcp_dispatch::service::ServiceBinding;
use std::io;
use zero_dns::{DnsBindingRevokedFuture, DnsCarrierBinding};
use zero_engine::GroupSelectionBinding;

pub(crate) struct NativeDnsBinding {
    pub(crate) carrier: ServiceBinding,
    pub(crate) selections: Vec<GroupSelectionBinding>,
}
impl std::fmt::Debug for NativeDnsBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDnsBinding")
            .field("carrier", &self.carrier)
            .field("selection_count", &self.selections.len())
            .finish()
    }
}
impl DnsCarrierBinding for NativeDnsBinding {
    fn check_current(&self) -> io::Result<()> {
        self.carrier.check_current().map_err(io::Error::other)?;
        for selection in &self.selections {
            selection.check_current().map_err(io::Error::other)?;
        }
        Ok(())
    }
    fn revoked(&self) -> DnsBindingRevokedFuture<'_> {
        use futures_util::{stream::FuturesUnordered, StreamExt};
        Box::pin(async move {
            if let Err(error) = self.check_current() {
                return error;
            }
            let mut changes: FuturesUnordered<_> = self
                .selections
                .iter()
                .map(|selection| selection.changed())
                .collect();
            tokio::select! {
                error = self.carrier.revoked() => io::Error::other(error),
                Some(error) = changes.next() => io::Error::other(error),
            }
        })
    }
}
