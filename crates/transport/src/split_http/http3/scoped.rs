//! H3 connection/request/body execution in the original accepted Scope.
use super::super::{
    body::{Body, Caller},
    request::Profile,
    server::{exchange, status},
    SplitHttpRegistry,
};
use crate::quic::{
    owned::{h3, OwnedConnection},
    source::Streams,
};
use bytes::Buf;
use futures_util::{stream::FuturesUnordered, StreamExt};
use http_body_util::BodyExt;
use std::{io, time::Duration};
use tokio::sync::mpsc;
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, BoxFuture, CancellationToken},
    AcceptedCaller, TcpRelayStream,
};
pub fn accept(
    connection: OwnedConnection,
    config: &(impl zero_traits::SplitHttpTransportProfile + ?Sized),
    registry: &SplitHttpRegistry,
    caller: &AcceptedCaller,
) -> io::Result<Streams> {
    let profile = Profile::new(config);
    let registry = registry.clone();
    let caller_copy = caller.clone();
    let (accepted, receiver) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let stopped = cancel.clone();
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            let result = run(
                connection,
                profile,
                registry,
                accepted,
                caller_copy,
                stopped,
            )
            .await;
            if let Err(error) = result {
                tracing::debug!(%error,"scoped H3 connection ended");
            }
            Ok(())
        })
        .map_err(io::Error::other)?;
    Ok(Streams {
        receiver: tokio::sync::Mutex::new(receiver),
        cancel,
    })
}
async fn run(
    connection: OwnedConnection,
    profile: Profile,
    registry: SplitHttpRegistry,
    accepted: mpsc::Sender<TcpRelayStream>,
    caller: AcceptedCaller,
    cancel: CancellationToken,
) -> io::Result<()> {
    let control = connection.try_clone().map_err(io::Error::other)?;
    let mut builder = ::h3::server::builder();
    builder.max_field_section_size(profile.options.server_max_header_bytes as u64);
    let mut server = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=builder.build(h3::Connection::new(connection))=>result.map_err(io::Error::other)?};
    let mut tasks = FuturesUnordered::<BoxFuture<'static, io::Result<()>>>::new();
    let mut accepting = true;
    loop {
        if !accepting && tasks.is_empty() {
            break;
        }
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            request=server.accept(),if accepting&&tasks.len()<64=>{
                let resolver=match request{Ok(Some(r))=>r,Ok(None)=>{accepting=false;continue},Err(error)=>{tracing::debug!(%error,"H3 request accept ended");break}};
                let profile=profile.clone();let registry=registry.clone();let accepted=accepted.clone();let caller=caller.clone();
                tasks.push(Box::pin(async move {
                    let (mut request,stream)=tokio::time::timeout(Duration::from_secs(30),resolver.resolve_request()).await.map_err(io::Error::other)?.map_err(io::Error::other)?;
                    request.extensions_mut().insert(Caller(caller.clone()));
                    let (mut send,mut recv)=stream.split();let (sender,receiver)=mpsc::channel(8);
                    let upload=super::super::body::spawn(Some(&Caller(caller)),async move {
                        let result=async {while let Some(mut data)=recv.recv_data().await.map_err(io::Error::other)?{let bytes=data.copy_to_bytes(data.remaining());sender.send(Ok(bytes)).await.map_err(io::Error::other)?;}Ok::<_,io::Error>(())}.await;
                        if let Err(error)=result{let _=sender.send(Err(error)).await;}
                    })?;
                    let (bridge,mut logical)=mpsc::channel(1);
                    let exchange=async {
                        let request=request.map(|_|Body{receiver:receiver.into(),task:Some(upload),life:None,progress:None});
                        let response=match registry.requests.clone().try_acquire_owned(){Ok(permit)=>exchange::handle(request,profile,registry.sessions,bridge,permit).await.unwrap_or_else(|error|{tracing::debug!(%error,"H3 request rejected");status(400)}),Err(_)=>status(503)};
                        let (parts,mut body)=response.into_parts();send.send_response(http::Response::from_parts(parts,())).await.map_err(io::Error::other)?;
                        while let Some(frame)=body.frame().await{if let Ok(data)=frame?.into_data(){send.send_data(data).await.map_err(io::Error::other)?;}}
                        send.finish().await.map_err(io::Error::other)
                    };
                    tokio::pin!(exchange);
                    loop{tokio::select!{result=&mut exchange=>return result,stream=logical.recv()=>{if let Some(stream)=stream{let _=accepted.try_send(TcpRelayStream::new(stream));}else{return exchange.await}}}}
                }));
            }
            result=tasks.next(),if !tasks.is_empty()=>{if let Some(Err(error))=result{tracing::debug!(%error,"H3 exchange ended");}},
        }
    }
    control.close(0x100u32.into(), b"");
    Ok(())
}
