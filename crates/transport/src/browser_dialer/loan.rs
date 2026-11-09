//! The exact hosted Browser stream/controller; no second native owner or pool.
use super::*;
use zero_platform_tokio::{
    contract::{
        lifecycle::{BoxChannelControl, CloseReport, DynChannelControl, Unfinished},
        BoxFuture, ChannelError, CloseReason, Impact, Reason, Stage,
    },
    controlled_carrier::loan::StreamLoan,
};
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};
/// Prepay both concrete boxes before selecting a Source or sending a task.
/// The same provider's immutable factory identifies the actual book pair.
pub struct PreparedBrowserLoan {
    dialer: BrowserDialer,
    book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    io: PacketMetadataCredit,
    control: PacketMetadataCredit,
    cancel: PacketMetadataCredit,
}
struct Control(BrowserOriginal);
fn failed(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component).with_source(source)
}
impl PreparedBrowserLoan {
    pub fn prepare(
        dialer: BrowserDialer,
        book: &zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        memory: &PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        let actual = dialer.inner.factory.as_ref().ok_or_else(|| {
            ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component)
        })?;
        if !actual.metadata.same_root(memory) || !actual.book.same_budget(book) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let io = memory
            .reserve(PacketMetadataLayout::boxed::<BrowserStream>())
            .map_err(failed)?;
        let control = memory
            .reserve(PacketMetadataLayout::boxed::<Control>())
            .map_err(failed)?;
        let cancel = memory
            .reserve(PacketMetadataLayout::boxed::<
                tokio_util::sync::WaitForCancellationFutureOwned,
            >())
            .map_err(failed)?;
        Ok(Self {
            dialer,
            book: book.clone(),
            io,
            control,
            cancel,
        })
    }
    /// Preserve the original Browser WS path/ed normalization while paying the
    /// URL, percent-decoding temporaries and re-encoded query before allocation.
    /// No copied header Vec or whole query-pair Vec is constructed here.
    pub async fn websocket_profile(
        self,
        profile: &crate::profile::OwnedWebSocketProfile,
        server: &str,
        port: u16,
        secure: bool,
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<StreamLoan, ChannelError> {
        if profile.host.as_deref().is_some_and(|host| !host.is_empty())
            || !profile.headers.is_empty()
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        let bound = profile
            .path
            .len()
            .checked_mul(8)
            .and_then(|size| size.checked_add(server.len()))
            .and_then(|size| size.checked_add(96))
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
            })?;
        let credit = self.book.try_reserve(bound).map_err(failed)?;
        let path = if profile.path.is_empty() {
            "/"
        } else {
            profile.path.as_str()
        };
        let mut normalized = String::with_capacity(path.len().saturating_mul(3).saturating_add(1));
        if !path.starts_with('/') {
            normalized.push('/');
        }
        if let Some((base, query)) = path.split_once('?').filter(|(_, query)| {
            url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| key == "ed")
        }) {
            normalized.push_str(base);
            let mut encoded = url::form_urlencoded::Serializer::new(String::with_capacity(
                query.len().saturating_mul(3),
            ));
            for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
                if key != "ed" {
                    encoded.append_pair(&key, &value);
                }
            }
            let query = encoded.finish();
            if !query.is_empty() {
                normalized.push('?');
                normalized.push_str(&query);
            }
        } else {
            normalized.push_str(path);
        }
        let mut url = String::with_capacity(
            server
                .len()
                .saturating_add(normalized.len())
                .saturating_add(24),
        );
        use std::fmt::Write;
        url.push_str(if secure { "wss://" } else { "ws://" });
        if server.contains(':') && !server.starts_with('[') {
            write!(&mut url, "[{server}]").unwrap();
        } else {
            url.push_str(server);
        }
        if !(secure && port == 443 || !secure && port == 80) {
            write!(&mut url, ":{port}").unwrap();
        }
        url.push_str(&normalized);
        let result = self
            .websocket(&url, None, profile.heartbeat_period_secs, upstream, call)
            .await;
        drop(url);
        drop(normalized);
        drop(credit);
        result
    }
    pub async fn websocket(
        self,
        url: &str,
        early_data: Option<&[u8]>,
        heartbeat: u32,
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<StreamLoan, ChannelError> {
        let stream = self
            .dialer
            .dial_websocket_owned(url, early_data, heartbeat, upstream, call)
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(source)
            })?;
        self.finish(stream, call.cancel.clone())
    }
    pub async fn get(
        self,
        url: &str,
        headers: &http::HeaderMap,
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<StreamLoan, ChannelError> {
        let stream = self
            .dialer
            .dial_get_owned(url, headers, upstream, call)
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(source)
            })?;
        self.finish(stream, call.cancel.clone())
    }
    fn finish(
        self,
        stream: BrowserStream,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<StreamLoan, ChannelError> {
        let stream = stream.bind_borrowed(cancel, self.cancel).map_err(failed)?;
        let original = stream.original().cloned().ok_or_else(|| {
            ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
        })?;
        let lineage = original.carrier_origin();
        let control = BoxChannelControl::from_prepaid(Control(original), self.control)
            .map_err(|(source, _, _)| failed(source))?;
        StreamLoan::from_prepaid_io(stream, control, lineage, self.io)
            .map_err(|(source, _, _, _, _)| failed(source))
    }
}
impl Control {
    fn report(&self) -> CloseReport {
        if self.0.is_settled() {
            CloseReport::default()
        } else {
            CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "same Browser accepted native/users/Scope/fault/Root return remains retained"
                        .into(),
                )],
                ..Default::default()
            }
        }
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.request_stop();
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.request_stop();
        Ok(self.report())
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.request_stop();
            match tokio::time::timeout_at(end, self.0 .0.lineage.wait_settled_original()).await {
                Ok(Ok(())) => Ok(self.report()),
                Ok(Err(source)) => Err(source),
                Err(_) => Ok(self.report()),
            }
        })
    }
}
