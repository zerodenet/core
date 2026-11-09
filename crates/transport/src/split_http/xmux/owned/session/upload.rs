//! Same payload/post semantics; scratch and every derived copy paid first.
use super::*;
use bytes::Bytes;
use zero_platform_tokio::controlled_carrier::{CarrierBufferCredit, CarrierBufferFrameCredit};
pub(super) fn transmit_peak(profile: &Profile, length: usize) -> io::Result<usize> {
    if !matches!(profile.mode, XhttpMode::PacketUp | XhttpMode::Auto)
        || !matches!(
            profile.options.uplink_data_placement.as_str(),
            "header" | "cookie"
        )
    {
        return Ok(length);
    }
    // Base64 String and retained headers/cookies coexist with old/new cookie
    // concatenation. All derived business-byte copies precede actual send.
    let encoded = length
        .checked_add(2)
        .and_then(|v| v.checked_div(3))
        .and_then(|v| v.checked_mul(4))
        .ok_or(io::ErrorKind::InvalidInput)?;
    let chunks = encoded.div_ceil(profile.options.uplink_chunk_size.from.max(64) as usize);
    let keys = profile
        .options
        .uplink_data_key
        .len()
        .checked_add(24)
        .and_then(|v| v.checked_mul(chunks))
        .ok_or(io::ErrorKind::InvalidInput)?;
    encoded
        .checked_mul(4)
        .and_then(|v| keys.checked_mul(3).and_then(|keys| v.checked_add(keys)))
        .and_then(|v| v.checked_add(length))
        .ok_or(io::ErrorKind::InvalidInput.into())
}
struct Scratch {
    bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
struct Frame {
    bytes: Vec<u8>,
    _credit: CarrierBufferFrameCredit,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
fn scratch(working: &mut Working) -> io::Result<Scratch> {
    let credit = working
        .take_receive_scratch()
        .ok_or(io::ErrorKind::InvalidInput)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(credit.bytes())
        .map_err(io::Error::other)?;
    bytes.resize(credit.bytes(), 0);
    Ok(Scratch {
        bytes,
        _credit: credit,
    })
}
fn copy(bytes: &[u8], credit: CarrierBufferFrameCredit) -> io::Result<Bytes> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(io::Error::other)?;
    owned.extend_from_slice(bytes);
    Ok(Bytes::from_owner(Frame {
        bytes: owned,
        _credit: credit,
    }))
}
pub(super) async fn produce(
    mut reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    mut sender: super::super::pipe::Sender,
    mut working: Working,
    life: crate::split_http::io::LifetimeRef,
) -> io::Result<()> {
    let mut scratch = scratch(&mut working)?;
    let lane = working.transmit();
    loop {
        let count = reader.read(&mut scratch.bytes).await?;
        if count == 0 {
            return Ok(());
        }
        let credit = lane.reserve(count).await?;
        sender
            .send(Ok(copy(&scratch.bytes[..count], credit)?))
            .await?;
        if life.is_closed() {
            return Ok(());
        }
    }
}
pub(super) struct Upload {
    pub(super) runtime: PacketMetadataArc<Runtime>,
    pub(super) sender: carrier::Sender,
    pub(super) profile: Profile,
    pub(super) session: [u8; 32],
    pub(super) reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    pub(super) working: Working,
    pub(super) life: crate::split_http::io::LifetimeRef,
    pub(super) causes: PacketMetadataArc<sources::RequestCauses>,
}
impl Upload {
    pub(super) async fn run(self) {
        let Self {
            runtime,
            mut sender,
            profile,
            session,
            mut reader,
            mut working,
            life,
            causes,
        } = self;
        let transfer = async {
            if profile.mode == XhttpMode::StreamUp {
                let (send, receive) = super::super::pipe::channel(&runtime.memory)?;
                let body = Body {
                    receiver: receive.into(),
                    task: None,
                    life: None,
                    progress: Some(life.clone()),
                };
                let request = profile.request(
                    &profile.options.uplink_http_method,
                    session_name(&session),
                    None,
                    body,
                )?;
                let produce = produce(reader, send, working, life.clone());
                let request = async { check_response(sender.send_request(request).await?).await };
                tokio::try_join!(produce, request)?;
                return Ok(());
            }
            let mut scratch = scratch(&mut working)?;
            let lane = working.transmit();
            let mut sequence = 0u64;
            let mut next = tokio::time::Instant::now();
            loop {
                let cap =
                    (crate::split_http::request::sample(profile.options.sc_max_each_post_bytes)
                        .max(1) as usize)
                        .min(scratch.bytes.len());
                let count = reader.read(&mut scratch.bytes[..cap]).await?;
                if count == 0 {
                    break;
                }
                let credit = lane.reserve(transmit_peak(&profile, count)?).await?;
                let bytes = copy(&scratch.bytes[..count], credit)?;
                tokio::time::sleep_until(next).await;
                // Same Bytes owner; this clone also holds derived-header credit
                // until the actual POST response was consumed, not just enqueued.
                let request = profile.packet_request_owned(
                    session_name(&session),
                    sequence,
                    bytes.clone(),
                )?;
                next = tokio::time::Instant::now()
                    + std::time::Duration::from_millis(crate::split_http::request::sample(
                        profile.options.sc_min_posts_interval_ms,
                    ) as u64);
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    sender.send_request(request),
                )
                .await
                .map_err(io::Error::other)??;
                tokio::time::timeout(std::time::Duration::from_secs(30), check_response(response))
                    .await
                    .map_err(io::Error::other)??;
                drop(bytes);
                life.commit(count);
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
            }
            Ok::<_, io::Error>(())
        };
        let result = tokio::select! {biased;_=life.cancelled()=>None,result=transfer=>Some(result)};
        if let Some(Err(source)) = result {
            let original = causes.record(0, source);
            life.fail_retained(original);
        }
    }
}
async fn check_response(
    response: http::Response<crate::split_http::body::ReceivedBody>,
) -> io::Result<()> {
    if response.status() != 200 {
        return Err(io::Error::other(sources::Status(
            "upload",
            response.status(),
        )));
    }
    let mut body = response.into_body();
    while let Some(frame) = body.frame().await {
        frame?;
    }
    Ok(())
}
