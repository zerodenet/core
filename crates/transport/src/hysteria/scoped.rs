//! Hysteria authentication, preface demux and H3 run under the actual caller.
use super::*;
use crate::quic::{
    owned::{h3 as owned_h3, OwnedBiStream, OwnedConnection},
    source::Streams,
};
use bytes::Bytes;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::{io::AsyncReadExt, sync::mpsc};
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, BoxFuture, CancellationToken},
    AcceptedCaller, TcpRelayStream,
};
pub fn accept(
    connection: OwnedConnection,
    profile: &Profile,
    caller: &AcceptedCaller,
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
) -> io::Result<Streams> {
    let authenticated = Arc::new(AtomicBool::new(false));
    let (web_tx, web_rx) = mpsc::channel(128);
    let (data_tx, data_rx) = mpsc::channel(128);
    let cancel = CancellationToken::new();
    let stopped = cancel.clone();
    let conn = connection.try_clone().map_err(io::Error::other)?;
    let auth = authenticated.clone();
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            let result = dispatch(conn, auth, web_tx, data_tx, stopped).await;
            result.map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Receive,
                    zero_platform_tokio::contract::Reason::Protocol,
                    zero_platform_tokio::contract::Impact::Session,
                )
                .with_source(source)
            })
        })
        .map_err(io::Error::other)?;
    let profile = profile.clone();
    let stopped = cancel.clone();
    let owner = caller.clone();
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            let result = serve(
                connection,
                profile,
                authenticated,
                web_rx,
                stopped,
                resources,
                owner,
            )
            .await;
            result.map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Handshake,
                    zero_platform_tokio::contract::Reason::Protocol,
                    zero_platform_tokio::contract::Impact::Session,
                )
                .with_source(source)
            })
        })
        .map_err(io::Error::other)?;
    Ok(Streams {
        receiver: tokio::sync::Mutex::new(data_rx),
        cancel,
    })
}
async fn dispatch(
    connection: OwnedConnection,
    authenticated: Arc<AtomicBool>,
    web: mpsc::Sender<owned_h3::BidiStream>,
    data: mpsc::Sender<TcpRelayStream>,
    cancel: CancellationToken,
) -> io::Result<()> {
    let mut tasks = FuturesUnordered::<BoxFuture<'static, io::Result<()>>>::new();
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            _=connection.closed()=>break,
            _=data.closed()=>break,
            pair=connection.accept_bi_with_connection(),if tasks.len()<128=>{
                let (conn,send,mut recv)=match pair{Ok(p)=>p,Err(_) if connection.close_reason().is_some()=>break,Err(source)=>return Err(io::Error::other(source))};let auth=authenticated.clone();let web=web.clone();let data=data.clone();
                tasks.push(Box::pin(async move {
                    let mut bytes=[0;8];let read=async {recv.read_exact(&mut bytes[..1]).await.map_err(io::Error::other)?;let length=1usize<<(bytes[0]>>6);if length>1{recv.read_exact(&mut bytes[1..length]).await.map_err(io::Error::other)?;}Ok::<_,io::Error>(length)};
                    let length=match tokio::time::timeout(std::time::Duration::from_secs(10),read).await{Ok(Ok(n))=>n,_=>return Ok(())};
                    let mut frame=u64::from(bytes[0]&63);for byte in &bytes[1..length]{frame=(frame<<8)|u64::from(*byte);}
                    if frame==0x401{if auth.load(Ordering::Acquire){let _=data.try_send(TcpRelayStream::new(OwnedBiStream::from_streams(conn,send,recv)));}}
                    else {let _=web.try_send(owned_h3::BidiStream::with_prefix(send,recv,Bytes::copy_from_slice(&bytes[..length])));}
                    Ok(())
                }));
            }
            _=tasks.next(),if !tasks.is_empty()=>{},
        }
    }
    connection.close(0x100u32.into(), b"");
    Ok(())
}
async fn serve(
    connection: OwnedConnection,
    profile: Profile,
    authenticated: Arc<AtomicBool>,
    web: mpsc::Receiver<owned_h3::BidiStream>,
    cancel: CancellationToken,
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    caller: AcceptedCaller,
) -> io::Result<()> {
    // All request callbacks only observe/authenticate this same control handle.
    // Its one actual user remains held until every callback returns; each real
    // bidi direction still retains its own independently charged permit.
    let owner = Arc::new(connection.try_clone().map_err(io::Error::other)?);
    let mut builder = ::h3::server::builder();
    builder.max_field_section_size(65536);
    let mut server = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=builder.build(owned_h3::Connection::with_incoming_bidi(connection,web))=>result.map_err(io::Error::other)?};
    let mut tasks = FuturesUnordered::<BoxFuture<'static, io::Result<()>>>::new();
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            request=server.accept(),if tasks.len()<64=>{
                let resolver=match request{Ok(Some(r))=>r,_=>break};let profile=profile.clone();let auth=authenticated.clone();let conn=owner.clone();let resources=resources.clone();let caller=caller.clone();
                tasks.push(Box::pin(async move {let (request,stream)=match tokio::time::timeout(std::time::Duration::from_secs(30),resolver.resolve_request()).await{Ok(Ok(r))=>r,_=>return Ok(())};super::request::serve_scoped(request,stream,&profile,&auth,|rate|profile.negotiate_owned(&conn,rate),resources.as_ref(),&caller).await}));
            }
            _=tasks.next(),if !tasks.is_empty()=>{},
        }
    }
    owner.close(0x100u32.into(), b"");
    Ok(())
}
