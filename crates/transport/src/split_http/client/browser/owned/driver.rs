//! Both directions are polled by the Factory's one registered original driver.
use super::*;
use bytes::Bytes;
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) fn control_peak(
    profile: &Profile,
    scheme: usize,
    authority: usize,
) -> io::Result<usize> {
    let mut size = scheme
        .checked_add(authority)
        .ok_or(io::ErrorKind::InvalidInput)?;
    for length in [
        profile.host.len(),
        profile.path.len(),
        profile.query.len(),
        profile.options.session_key.len(),
        profile.options.seq_key.len(),
        profile.options.x_padding_key.len(),
        profile.options.x_padding_header.len(),
    ] {
        size = size
            .checked_add(length)
            .ok_or(io::ErrorKind::InvalidInput)?;
    }
    for (name, value) in &profile.options.headers {
        size = size
            .checked_add(name.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(io::ErrorKind::InvalidInput)?;
    }
    size.checked_add(profile.options.x_padding_bytes.to as usize)
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(profile.options.server_max_header_bytes as usize))
        .and_then(|n| n.checked_add(512))
        .ok_or(io::ErrorKind::InvalidInput.into())
}
impl BrowserXhttpDriver {
    /// The Factory has already published the exact driver TaskJoinWitness and
    /// bound this upstream to its actual local Source pending receiver.
    pub async fn run(
        mut self,
        mut get: CarrierIo,
        upstream: UpstreamTransport,
        call: CarrierContext,
    ) -> Result<(), ChannelError> {
        let network = std::sync::Mutex::new(self.network);
        let download = async {
            let mut writer = network::Writer(&network);
            loop {
                let size = get.read(&mut self.download.bytes).await?;
                if size == 0 {
                    writer.shutdown().await?;
                    return Ok(());
                }
                writer.write_all(&self.download.bytes[..size]).await?;
            }
        };
        let upload = Upload {
            profile: &self.profile,
            scheme: &self.scheme,
            authority: &self.authority,
            session: &self.session,
            dialer: &self.dialer,
            reader: network::Reader(&network),
            buffer: &mut self.upload.bytes,
            transmit: &self.transmit,
            control: &self.control,
            life: &self.life,
            upstream: &upstream,
            call: &call,
        }
        .run();
        let (down, up) = tokio::join!(
            direction(download, self.life.clone(), self.causes.clone(), 0, &call),
            direction(upload, self.life.clone(), self.causes.clone(), 1, &call),
        );
        // Logical return says only that this original network work has ended.
        // The Factory still receives the actual Scope and all Source receipts.
        self._returned.request_return();
        if down || up {
            return Err(
                ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(
                    crate::split_http::xmux::owned::request::original_io(self.causes.into_error()),
                ),
            );
        }
        Ok(())
    }
}
async fn direction(
    work: impl Future<Output = io::Result<()>>,
    life: LifetimeRef,
    causes: PacketMetadataArc<RequestCauses>,
    direction: usize,
    call: &CarrierContext,
) -> bool {
    let result = tokio::select! {biased;
        _=call.cancel.cancelled()=>None,
        result=work=>Some(result),
        _=life.cancelled()=>None,
    };
    if let Some(Err(source)) = result {
        let source = causes.record(direction, source);
        life.fail_retained(source);
        true
    } else {
        false
    }
}
struct Upload<'a> {
    profile: &'a Profile,
    scheme: &'a str,
    authority: &'a str,
    session: &'a [u8; 32],
    dialer: &'a BrowserDialer,
    reader: network::Reader<'a>,
    buffer: &'a mut [u8],
    transmit: &'a CarrierBufferWorkingLane,
    control: &'a CarrierBufferWorkingLane,
    life: &'a LifetimeRef,
    upstream: &'a UpstreamTransport,
    call: &'a CarrierContext,
}
impl Upload<'_> {
    async fn run(self) -> io::Result<()> {
        let Self {
            profile,
            scheme,
            authority,
            session,
            dialer,
            mut reader,
            buffer,
            transmit,
            control,
            life,
            upstream,
            call,
        } = self;
        let session = std::str::from_utf8(session).expect("fixed lowercase hex");
        let mut sequence = 0u64;
        let mut next = tokio::time::Instant::now();
        loop {
            let cap = (crate::split_http::request::sample(profile.options.sc_max_each_post_bytes)
                .max(1) as usize)
                .min(buffer.len());
            let size = reader.read(&mut buffer[..cap]).await?;
            if size == 0 {
                return Ok(());
            }
            tokio::time::sleep_until(next).await;
            // The original frame ticket outlives both Bytes views and all derived
            // headers. No collected body Vec is constructed for a known one-frame body.
            let _frame = transmit.reserve(profile.transmit_peak(size)?).await?;
            let _control = control.reserve(control.bound()).await?;
            let bytes = Bytes::copy_from_slice(&buffer[..size]);
            let request = profile.packet_request_owned(session, sequence, bytes.clone())?;
            let (parts, _body) = request.into_parts();
            let url = super::super::absolute_url(scheme, authority, &parts.uri)?;
            let embedded = matches!(
                profile.options.uplink_data_placement.as_str(),
                "header" | "cookie"
            );
            let payload = if embedded { &[][..] } else { bytes.as_ref() };
            // Assignment/final ACK errors are terminal. The same real Source must
            // leave its original global book before the next ordered POST opens.
            dialer
                .send_packet_owned(&parts.method, &url, &parts.headers, payload, upstream, call)
                .await?;
            life.commit(size);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
            next = tokio::time::Instant::now()
                + Duration::from_millis(crate::split_http::request::sample(
                    profile.options.sc_min_posts_interval_ms,
                ) as u64);
        }
    }
}
