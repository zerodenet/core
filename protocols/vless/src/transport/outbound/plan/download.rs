use super::*;
use crate::transport::{VlessTransportRuntime, VlessXhttpDownloadOptionsRef};
impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) fn set_download<
        TTls: ClientTlsProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    >(
        &mut self,
        download: Option<VlessXhttpDownloadOptionsRef<'_, TTls, TSplit>>,
        runtime: &VlessTransportRuntime,
        tag: &str,
    ) {
        let Some(download) = download else {
            return;
        };
        let reality = download.reality.map(VlessRealityClientProfile::from);
        let quic = download.quic.map(VlessQuicClientProfile::from);
        let mut plan = Self::from_profile_refs(
            self.transport.source_dir.as_deref(),
            download.server,
            download.port,
            download.tls,
            reality.as_ref(),
            None::<&OwnedWebSocketProfile>,
            None::<&OwnedGrpcProfile>,
            None::<&OwnedH2Profile>,
            None::<&OwnedHttpUpgradeProfile>,
            Some(download.split_http),
            quic.as_ref(),
        );
        plan.share_browser_dialer(runtime);
        plan.share_xhttp_pool(runtime, tag);
        self.download = Some(Box::new(plan));
    }
}
