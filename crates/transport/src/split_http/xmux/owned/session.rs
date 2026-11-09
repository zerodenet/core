//! Original logical transfer work stays in the actual shared H3 driver.
mod upload;
use super::*;
use crate::split_http::{
    body::Body,
    client::{self, carrier},
    io::XhttpStream,
    request::Profile,
    XhttpMode,
};
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::controlled_carrier::{CarrierBufferCredit, CarrierBufferWorkingLane};
struct Working {
    scratch: Option<CarrierBufferCredit>,
    transmit: CarrierBufferWorkingLane,
}
impl Working {
    fn take_receive_scratch(&mut self) -> Option<CarrierBufferCredit> {
        self.scratch.take()
    }
    fn transmit(&self) -> CarrierBufferWorkingLane {
        self.transmit.clone()
    }
}
impl Runtime {
    pub(in crate::split_http) async fn connect_profiles(
        runtime: &PacketMetadataArc<Self>,
        mut upload: carrier::Sender,
        mut download: carrier::Sender,
        profile: Profile,
        download_profile: Profile,
    ) -> io::Result<XhttpStream> {
        if runtime.stopped.is_cancelled() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let duplex = runtime.book.try_reserve(2 * 64 * 1024)?;
        let scratch = if matches!(profile.mode, XhttpMode::PacketUp | XhttpMode::Auto) {
            profile.options.sc_max_each_post_bytes.to.max(1) as usize
        } else {
            16 * 1024
        };
        let transmit = upload::transmit_peak(&profile, scratch)?;
        let working = Working {
            scratch: Some(runtime.book.try_reserve(scratch)?),
            transmit: runtime.book.try_working_lane(transmit)?,
        };
        let causes = sources::RequestCauses::prepare(&runtime.memory)?;
        let life = crate::split_http::io::LifetimeRef::prepare(&runtime.memory)?;
        let (mut stream, network) = client::stream_pair_with_lifetime(life.clone());
        let returned = super::OwnedLogicalReturn::prepare(&runtime.memory, life.clone(), duplex)?;
        stream.owned_return = Some(returned.clone());
        upload.bind_logical(returned.clone())?;
        download.bind_logical(returned.clone())?;
        let (reader, writer) = tokio::io::split(network);
        let session = session_id(rand::random::<u128>());
        if profile.mode == XhttpMode::StreamOne {
            let (sender, receiver) = super::pipe::channel(&runtime.memory)?;
            let body = Body {
                receiver: receiver.into(),
                task: None,
                life: None,
                progress: Some(life.clone()),
            };
            let request = profile.request(&profile.options.uplink_http_method, "", None, body)?;
            let work_returned = returned.enter()?;
            let operation = Single {
                upload,
                request,
                reader,
                writer,
                sender,
                working,
                life: life.clone(),
                causes: causes.clone(),
            }
            .run();
            runtime
                .ledger
                .push(Work::Session(PaidFuture::prepare_returning(
                    operation,
                    &runtime.memory,
                    Some(work_returned),
                )?))?;
            return Ok(stream);
        }
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            download.send_request(download_profile.request(
                "GET",
                session_name(&session),
                None,
                Body::empty(),
            )?),
        )
        .await
        .map_err(io::Error::other)??;
        if response.status() != 200 {
            return Err(io::Error::other(sources::Status(
                "download",
                response.status(),
            )));
        }
        let down_life = life.clone();
        let down_sources = causes.clone();
        let down_returned = returned.enter()?;
        let down = async move {
            let result = async {
                let mut writer = writer;
                let mut body = response.into_body();
                while let Some(frame) = body.frame().await {
                    if let Ok(data) = frame?.into_data() {
                        writer.write_all(&data).await?;
                    }
                }
                writer.shutdown().await
            }
            .await;
            if let Err(source) = result {
                let original = down_sources.record(1, source);
                down_life.fail_retained(original);
            }
        };
        runtime
            .ledger
            .push(Work::Session(PaidFuture::prepare_returning(
                cancel_on_logical(life.clone(), down),
                &runtime.memory,
                Some(down_returned),
            )?))?;
        let up_returned = returned.enter()?;
        let upload = upload::Upload {
            runtime: runtime.clone(),
            sender: upload,
            profile,
            session,
            reader,
            working,
            life: life.clone(),
            causes,
        }
        .run();
        runtime
            .ledger
            .push(Work::Session(PaidFuture::prepare_returning(
                upload,
                &runtime.memory,
                Some(up_returned),
            )?))?;
        Ok(stream)
    }
}
async fn cancel_on_logical(
    life: crate::split_http::io::LifetimeRef,
    work: impl std::future::Future<Output = ()>,
) {
    tokio::select! {biased;_=life.cancelled()=>{},_=work=>{}}
}
struct Single {
    upload: carrier::Sender,
    request: http::Request<Body>,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    sender: super::pipe::Sender,
    working: Working,
    life: crate::split_http::io::LifetimeRef,
    causes: PacketMetadataArc<sources::RequestCauses>,
}
impl Single {
    async fn run(self) {
        let Self {
            mut upload,
            request,
            reader,
            mut writer,
            sender,
            working,
            life,
            causes,
        } = self;
        let up = upload::produce(reader, sender, working, life.clone());
        let down = async {
            let response = upload.send_request(request).await?;
            if response.status() != 200 {
                return Err(io::Error::other(sources::Status(
                    "stream-one",
                    response.status(),
                )));
            }
            let mut body = response.into_body();
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    writer.write_all(&data).await?;
                }
            }
            writer.shutdown().await
        };
        let record = |direction, result: io::Result<()>| {
            if let Err(source) = result {
                let original = causes.record(direction, source);
                life.fail_retained(original);
            }
        };
        let up = async { record(0, up.await) };
        let down = async { record(1, down.await) };
        cancel_on_logical(life.clone(), async {
            tokio::join!(up, down);
        })
        .await;
    }
}
fn session_id(value: u128) -> [u8; 32] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = [0; 32];
    for (index, value) in value.to_be_bytes().into_iter().enumerate() {
        result[index * 2] = HEX[(value >> 4) as usize];
        result[index * 2 + 1] = HEX[(value & 15) as usize];
    }
    result
}
fn session_name(value: &[u8; 32]) -> &str {
    std::str::from_utf8(value).expect("only fixed hexadecimal digits")
}
