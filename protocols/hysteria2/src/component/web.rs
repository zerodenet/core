//! H3 authentication and preface classification belong to the accepted Scope.
use super::{
    connection::{error, failure},
    *,
};
use bytes::{Buf, Bytes};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, oneshot},
};
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, BoxFuture, CancellationToken, ChannelError},
    AcceptedCaller, TcpRelayStream,
};
use zero_transport::quic::owned::{h3 as owned_h3, OwnedBiStream, OwnedConnection};
struct Auth {
    authenticated: AtomicBool,
    session: Mutex<Option<zero_core::SessionAuth>>,
    sender: Mutex<Option<oneshot::Sender<zero_core::SessionAuth>>>,
}
pub(super) async fn accept(
    connection: OwnedConnection,
    profile: Hysteria2AuthenticatedInboundProfile,
    caller: &AcceptedCaller,
    resources: Arc<dyn ResourceAccess>,
    cancel: CancellationToken,
) -> Result<(zero_core::SessionAuth, mpsc::Receiver<TcpRelayStream>), ChannelError> {
    let (web_tx, web_rx) = mpsc::channel(128);
    let (tcp_tx, tcp_rx) = mpsc::channel(128);
    let (sender, auth) = oneshot::channel();
    let state = Arc::new(Auth {
        authenticated: AtomicBool::new(false),
        session: Mutex::new(None),
        sender: Mutex::new(Some(sender)),
    });
    let conn = connection.try_clone()?;
    let stop = cancel.clone();
    let auth_state = state.clone();
    caller.spawn(TaskClass::Auxiliary, move |_| async move {
        let _exit = stop.clone().drop_guard();
        let _ = dispatch(conn, auth_state, web_tx, tcp_tx, stop).await;
        Ok(())
    })?;
    let owner = caller.clone();
    let stop = cancel.clone();
    caller.spawn(TaskClass::Auxiliary, move |_| async move {
        let _exit = stop.clone().drop_guard();
        let _ = serve(connection, profile, state, web_rx, stop, resources, owner).await;
        Ok(())
    })?;
    let auth = auth
        .await
        .map_err(|_| error("connection ended before authentication"))?;
    Ok((auth, tcp_rx))
}
async fn dispatch(
    connection: OwnedConnection,
    state: Arc<Auth>,
    web: mpsc::Sender<owned_h3::BidiStream>,
    tcp: mpsc::Sender<TcpRelayStream>,
    cancel: CancellationToken,
) -> Result<(), ChannelError> {
    let mut pending = FuturesUnordered::<BoxFuture<'static, Result<(), ChannelError>>>::new();
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            _=tcp.closed()=>break,
            pair=connection.accept_bi(),if pending.len()<128=>{
                let (send,mut recv)=match pair{Ok(pair)=>pair,Err(_)=>break};let conn=connection.try_clone()?;let state=state.clone();let web=web.clone();let tcp=tcp.clone();
                pending.push(Box::pin(async move {
                    let mut prefix=[0;8];let read=async {recv.read_exact(&mut prefix[..1]).await.map_err(failure)?;let size=1usize<<(prefix[0]>>6);if size>1 {recv.read_exact(&mut prefix[1..size]).await.map_err(failure)?;}Ok::<_,ChannelError>(size)};
                    let size=match tokio::time::timeout(std::time::Duration::from_secs(10),read).await{Ok(Ok(size))=>size,_=>return Ok(())};
                    let frame=crate::shared::decode_varint(&prefix[..size]).map_err(failure)?.0;
                    if frame==0x401 {if state.authenticated.load(Ordering::Acquire) {
                        let stream=zero_transport::ReplayStream::new(OwnedBiStream::from_streams(conn,send,recv),prefix[..size].to_vec());
                        let _=tcp.send(TcpRelayStream::new(stream)).await;
                    }} else {let _=web.send(owned_h3::BidiStream::with_prefix(send,recv,Bytes::copy_from_slice(&prefix[..size]))).await;}
                    Ok(())
                }));
            },
            _=pending.next(),if !pending.is_empty()=>{},
        }
    }
    connection.close(0x100u32.into(), b"");
    Ok(())
}
async fn serve(
    connection: OwnedConnection,
    profile: Hysteria2AuthenticatedInboundProfile,
    state: Arc<Auth>,
    web: mpsc::Receiver<owned_h3::BidiStream>,
    cancel: CancellationToken,
    resources: Arc<dyn ResourceAccess>,
    caller: AcceptedCaller,
) -> io::Result<()> {
    let owner = connection.try_clone().map_err(io::Error::other)?;
    let builder = h3::server::builder();
    let mut server = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=builder.build(owned_h3::Connection::with_incoming_bidi(connection,web))=>result.map_err(io::Error::other)?};
    let mut pending = FuturesUnordered::<BoxFuture<'static, io::Result<()>>>::new();
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            request=server.accept(),if pending.len()<64=>{
                let resolver=match request{Ok(Some(resolver))=>resolver,_=>break};let conn=owner.try_clone().map_err(io::Error::other)?;let profile=profile.clone();let state=state.clone();let resources=resources.clone();let caller=caller.clone();
                pending.push(Box::pin(async move {let work=async {let (request,mut stream)=resolver.resolve_request().await.map_err(io::Error::other)?;request_using(request,&mut Exchange(&mut stream),&profile,&state,&conn,resources.as_ref(),&caller).await};let _=tokio::time::timeout(std::time::Duration::from_secs(30),work).await;Ok(())}));
            },
            _=pending.next(),if !pending.is_empty()=>{},
        }
    }
    owner.close(0x100u32.into(), b"");
    Ok(())
}
async fn request_using(
    mut request: http::Request<()>,
    stream: &mut dyn zero_transport::http_server::HttpExchange,
    profile: &Hysteria2AuthenticatedInboundProfile,
    state: &Auth,
    connection: &OwnedConnection,
    resources: &dyn ResourceAccess,
    caller: &AcceptedCaller,
) -> io::Result<()> {
    let is_auth = request.method() == http::Method::POST
        && request.uri().path() == "/auth"
        && request
            .uri()
            .authority()
            .is_some_and(|a| a.host() == "hysteria");
    let auth = if is_auth {
        let mut current = state.session.lock().unwrap();
        if current.is_none() {
            *current = request
                .headers()
                .get("hysteria-auth")
                .and_then(|v| v.to_str().ok())
                .and_then(|password| profile.protocol.authenticate_password(password).ok());
            if current.is_some() {
                let receive = crate::handshake::AuthResponse::from_headers(
                    None,
                    request
                        .headers()
                        .get("hysteria-cc-rx")
                        .and_then(|v| v.to_str().ok()),
                )
                .receive_bandwidth;
                let receive = match receive {
                    crate::handshake::ReceiveBandwidth::Limit(rate) => rate,
                    _ => 0,
                };
                connection.negotiate_rate(profile.settings.server_send_rate(receive));
                state.authenticated.store(true, Ordering::Release);
            }
        }
        current.clone()
    } else {
        None
    };
    if let Some(auth) = auth {
        let receive = if profile.settings.ignore_client_bandwidth {
            "auto".into()
        } else {
            profile.settings.download.to_string()
        };
        stream
            .send_response(
                http::Response::builder()
                    .status(233)
                    .header("Hysteria-UDP", "true")
                    .header("Hysteria-CC-RX", receive)
                    .body(())
                    .map_err(io::Error::other)?,
            )
            .await?;
        stream.finish().await?;
        if let Some(sender) = state.sender.lock().unwrap().take() {
            let _ = sender.send(auth);
        }
        Ok(())
    } else {
        request
            .extensions_mut()
            .insert(zero_transport::http_server::RequestContext {
                peer: connection.remote_address(),
                tls: true,
            });
        profile
            .masquerade
            .serve_scoped(request, stream, resources, caller)
            .await
    }
}
struct Exchange<'a>(&'a mut h3::server::RequestStream<owned_h3::BidiStream, Bytes>);
#[async_trait::async_trait]
impl zero_transport::http_server::HttpExchange for Exchange<'_> {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        Ok(self
            .0
            .recv_data()
            .await
            .map_err(io::Error::other)?
            .map(|mut data| {
                let size = data.remaining();
                data.copy_to_bytes(size)
            }))
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.0
            .send_response(response)
            .await
            .map_err(io::Error::other)
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.0.send_data(data).await.map_err(io::Error::other)
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.0.finish().await.map_err(io::Error::other)
    }
}
