//! Original H3 request upload/download work, polled in its same driver ledger.
use super::*;
use crate::{
    quic::owned::h3::RequestSender,
    split_http::body::{Body, ReceivedBody},
};
use bytes::{Buf, Bytes};
use http_body_util::BodyExt;
use tokio::sync::oneshot;
use zero_platform_tokio::controlled_carrier::{CarrierBufferFrameCredit, CarrierBufferWorkingLane};
use zero_traits::packet_metadata::{PacketMetadataError, PacketMetadataWeak};
#[derive(Clone)]
pub(crate) struct Sender {
    sender: RequestSender,
    runtime: PacketMetadataWeak<Runtime>,
    logical: Option<super::OwnedLogicalReturn>,
}
impl Sender {
    pub(super) fn new(sender: RequestSender, runtime: &PacketMetadataArc<Runtime>) -> Self {
        Self {
            sender,
            runtime: PacketMetadataArc::downgrade(runtime),
            logical: None,
        }
    }
    pub(in crate::split_http) fn bind_logical(&mut self, original: super::OwnedLogicalReturn) {
        self.logical = Some(original);
    }
    pub async fn send_request(
        &mut self,
        request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        let runtime = self.runtime.upgrade().ok_or(io::ErrorKind::BrokenPipe)?;
        if runtime.stopped.is_cancelled() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let sources = sources::RequestCauses::prepare(&runtime.memory)?;
        // Guarantee the first real response copy before opening its request.
        // Later copies/queued outputs use this same lane/root on demand.
        let receive = runtime.book.try_working_lane(16 * 1024)?;
        let (mut parts, body) = request.into_parts();
        let host = parts
            .headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("localhost");
        parts.uri = format!(
            "https://{host}{}",
            parts
                .uri
                .path_and_query()
                .map(|value| value.as_str())
                .unwrap_or("/")
        )
        .parse()
        .map_err(io::Error::other)?;
        let response_memory = runtime
            .memory
            .reserve(zero_traits::packet_metadata::PacketMetadataLayout::boxed::<
                ResponseBody,
            >())
            .map_err(io::Error::other)?;
        let (outputs, received) = super::pipe::channel(&runtime.memory)?;
        let (ready, receiver) = oneshot::channel();
        let stop = CancellationToken::new();
        let logical = self
            .logical
            .clone()
            .ok_or(io::ErrorKind::PermissionDenied)?;
        let returned = logical.enter()?;
        let operation = Request {
            sender: self.sender.clone(),
            headers: http::Request::from_parts(parts, ()),
            body,
            runtime: runtime.clone(),
            sources,
            receive,
            stop,
            ready,
            response_memory,
            outputs,
            receiver: received,
        }
        .run();
        let work = async move {
            // Ready producer results are consumed first. Cancellation ends only
            // this request; its original stream views drop before WorkReturn.
            tokio::select! { biased; _=operation=>{}, _=logical.cancelled()=>{} }
        };
        runtime
            .ledger
            .push(Work::Request(PaidFuture::prepare_returning(
                work,
                &runtime.memory,
                Some(returned),
            )?))?;
        receiver.await.map_err(io::Error::other)?
    }
}
struct Request {
    sender: RequestSender,
    headers: http::Request<()>,
    body: Body,
    runtime: PacketMetadataArc<Runtime>,
    sources: PacketMetadataArc<sources::RequestCauses>,
    receive: CarrierBufferWorkingLane,
    stop: CancellationToken,
    ready: oneshot::Sender<io::Result<http::Response<ReceivedBody>>>,
    response_memory: zero_traits::packet_metadata::PacketMetadataCredit,
    outputs: super::pipe::Sender,
    receiver: super::pipe::Receiver,
}
impl Request {
    async fn run(self) {
        let Self {
            mut sender,
            headers,
            mut body,
            runtime,
            sources,
            receive,
            stop,
            ready,
            response_memory,
            mut outputs,
            receiver,
        } = self;
        let stream = tokio::select! {biased;
            _=runtime.stopped.cancelled()=>return,
            result=sender.send_request(headers)=>match result {
                Ok(stream)=>stream,
                Err(source)=>{let original=sources.record(0,io::Error::other(source));let _=ready.send(Err(original_io(original)));return;}
            },
        };
        let (mut send, mut recv) = stream.split();
        let ready = std::sync::Mutex::new(Some(ready));
        let stopped = stop.clone();
        let upload = async {
            let work = async {
                while let Some(frame) = body.frame().await {
                    if let Ok(data) = frame?.into_data() {
                        send.send_data(data).await.map_err(io::Error::other)?;
                    }
                }
                send.finish().await.map_err(io::Error::other)
            };
            let result = tokio::select! {biased;result=work=>Some(result),_=stopped.cancelled()=>None,_=runtime.stopped.cancelled()=>None};
            if let Some(Err(source)) = result {
                let original = sources.record(0, source);
                let response = {
                    ready
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                };
                if let Some(response) = response {
                    let _ = response.send(Err(original_io(original)));
                }
                stopped.cancel();
            }
        };
        let download = async {
            let work = async {
                let response = recv.recv_response().await.map_err(io::Error::other)?;
                let response = response.map(|_| {
                    ReceivedBody::OwnedH3(PaidResponseBody {
                        inner: Box::new(ResponseBody {
                            body: Body {
                                receiver: receiver.into(),
                                task: Some(crate::split_http::body::Task::Scoped(stop.clone())),
                                life: None,
                                progress: None,
                            },
                            sources: sources.clone(),
                            failure_returned: false,
                        }),
                        _memory: response_memory,
                    })
                });
                let handoff = {
                    ready
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                };
                if handoff.is_none_or(|handoff| handoff.send(Ok(response)).is_err()) {
                    stop.cancel();
                    return Ok(());
                }
                while let Some(mut data) = recv.recv_data().await.map_err(io::Error::other)? {
                    while data.has_remaining() {
                        let length = data.remaining().min(16 * 1024);
                        let credit = receive.reserve(length).await?;
                        let mut bytes = Vec::new();
                        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
                        bytes.resize(length, 0);
                        data.copy_to_slice(&mut bytes);
                        let frame = Bytes::from_owner(Frame {
                            bytes,
                            _credit: credit,
                        });
                        outputs.send(Ok(frame)).await.map_err(io::Error::other)?;
                    }
                }
                Ok::<_, io::Error>(())
            };
            let result = tokio::select! {biased;result=work=>Some(result),_=stop.cancelled()=>None,_=runtime.stopped.cancelled()=>None};
            if let Some(Err(source)) = result {
                let original = sources.record(1, source);
                let response = {
                    ready
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                };
                if let Some(ready) = response {
                    let _ = ready.send(Err(original_io(original)));
                }
                // The returned body owns the same pure sources. Once buffered DATA
                // drains, it returns this exact typed failure rather than false EOF.
                // Closing this request never waits for space in its output FIFO.
                stop.cancel();
            }
        };
        tokio::join!(upload, download);
        // No task/ACK/native claim is made here; both original request directions
        // have returned their views to the same sealed owned Endpoint ledger.
    }
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
#[derive(Debug)]
struct OriginalIo(PacketMetadataError);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_error(), f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_error())
    }
}
pub(in crate::split_http) fn original_io(source: PacketMetadataError) -> io::Error {
    let kind = source
        .as_error()
        .downcast_ref::<io::Error>()
        .map_or(io::ErrorKind::Other, io::Error::kind);
    io::Error::new(kind, OriginalIo(source))
}

pub(in crate::split_http) struct PaidResponseBody {
    inner: Box<ResponseBody>,
    _memory: zero_traits::packet_metadata::PacketMetadataCredit,
}
struct ResponseBody {
    body: Body,
    sources: PacketMetadataArc<sources::RequestCauses>,
    failure_returned: bool,
}
impl hyper::body::Body for PaidResponseBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        let body = &mut *self.inner;
        match hyper::body::Body::poll_frame(std::pin::Pin::new(&mut body.body), cx) {
            Poll::Ready(None) if !body.failure_returned => {
                if let Some(source) = body.sources.get(0).or_else(|| body.sources.get(1)).cloned() {
                    body.failure_returned = true;
                    Poll::Ready(Some(Err(original_io(source))))
                } else {
                    Poll::Ready(None)
                }
            }
            result => result,
        }
    }
}
