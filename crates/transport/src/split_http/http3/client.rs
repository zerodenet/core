use super::super::{
    body::{Body, ReceivedBody},
    client::{self, carrier},
    io::XhttpStream,
    request::Profile,
    XhttpMode,
};
use bytes::{Buf, Bytes};
use http_body_util::BodyExt;
use std::{future::poll_fn, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::SplitHttpTransportProfile;

type RequestSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
#[derive(Clone)]
pub(in crate::split_http) struct Sender(RequestSender);
struct TaskGuard(tokio::task::AbortHandle);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Sender {
    pub(in crate::split_http) async fn send_request(
        &mut self,
        request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        let (mut parts, mut body) = request.into_parts();
        let host = parts
            .headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("localhost");
        parts.uri = format!(
            "https://{host}{}",
            parts
                .uri
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/")
        )
        .parse()
        .map_err(io::Error::other)?;
        let stream = self
            .0
            .send_request(http::Request::from_parts(parts, ()))
            .await
            .map_err(io::Error::other)?;
        let (mut send, mut recv) = stream.split();
        let upload = tokio::spawn(async move {
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    send.send_data(data).await.map_err(io::Error::other)?;
                }
            }
            send.finish().await.map_err(io::Error::other)
        });
        let guard = TaskGuard(upload.abort_handle());
        let response = recv.recv_response().await.map_err(io::Error::other)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        // The last H3 sender closes its connection, so retain one until this
        // response body is dropped, including while the upload finishes.
        let keepalive = self.0.clone();
        let driver = tokio::spawn(async move {
            let _guard = guard;
            let _keepalive = keepalive;
            let result = async {
                while let Some(mut data) = recv.recv_data().await.map_err(io::Error::other)? {
                    let bytes = data.copy_to_bytes(data.remaining());
                    sender.send(Ok(bytes)).await.map_err(io::Error::other)?;
                }
                Ok::<_, io::Error>(())
            }
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(response.map(|_| {
            ReceivedBody::H3(Body {
                receiver,
                task: Some(driver.abort_handle()),
                life: None,
                progress: None,
            })
        }))
    }
}
async fn open(connection: quinn::Connection, stream: &mut XhttpStream) -> io::Result<Sender> {
    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(connection))
        .await
        .map_err(io::Error::other)?;
    let state = stream.life.clone();
    stream.tasks.push(
        tokio::spawn(async move {
            tokio::select! {
                error = poll_fn(|cx|driver.poll_close(cx)) => state.fail(error),
                _ = state.cancelled() => {},
            }
        })
        .abort_handle(),
    );
    Ok(Sender(sender))
}
pub async fn connect_xhttp_h3<P: SplitHttpTransportProfile + ?Sized>(
    connection: quinn::Connection,
    config: &P,
) -> Result<XhttpStream, crate::RuntimeError> {
    let profile = Profile::new(config);
    let (mut stream, network) = client::stream_pair();
    let sender = open(connection, &mut stream).await?;
    if profile.mode == XhttpMode::StreamOne {
        return single(carrier::Sender::Http3(sender), stream, network, profile);
    }
    let download = Sender(sender.0.clone());
    client::connect_channels(
        carrier::Sender::Http3(sender),
        carrier::Sender::Http3(download),
        stream,
        network,
        profile,
    )
    .await
}
pub(in crate::split_http) fn single(
    mut sender: carrier::Sender,
    mut stream: XhttpStream,
    network: tokio::io::DuplexStream,
    profile: Profile,
) -> Result<XhttpStream, crate::RuntimeError> {
    let (mut reader, mut writer) = tokio::io::split(network);
    let state = stream.life.clone();
    let (tx, receiver) = tokio::sync::mpsc::channel(8);
    let producer = tokio::spawn(async move {
        let mut buffer = vec![0; 16 * 1024];
        loop {
            let size = reader.read(&mut buffer).await?;
            if size == 0 {
                return Ok::<_, io::Error>(());
            }
            tx.send(Ok(Bytes::copy_from_slice(&buffer[..size])))
                .await
                .map_err(io::Error::other)?;
        }
    });
    let body = Body {
        receiver,
        task: Some(producer.abort_handle()),
        life: None,
        progress: Some(state.clone()),
    };
    let request = profile.request(&profile.options.uplink_http_method, "", None, body)?;
    stream.tasks.push(tokio::spawn(async move {
        let transfer = async {
            let response = sender.send_request(request).await?;
            if response.status() != 200 { return Err(io::Error::other(format!("xhttp stream-one status {}",response.status()))); }
            let mut body = response.into_body();
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() { writer.write_all(&data).await?; }
            }
            writer.shutdown().await
        };
        tokio::select! { result = transfer => { if let Err(error) = result { state.fail(error); } }, _ = state.cancelled() => {} }
    }).abort_handle());
    Ok(stream)
}

pub(in crate::split_http) async fn open_shared(
    connection: quinn::Connection,
    closed: impl FnOnce() + Send + 'static,
) -> io::Result<(Sender, tokio::task::AbortHandle)> {
    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(connection))
        .await
        .map_err(io::Error::other)?;
    let task = tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
        closed();
    });
    Ok((Sender(sender), task.abort_handle()))
}
