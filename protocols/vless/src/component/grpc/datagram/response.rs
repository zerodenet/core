//! Reuse the protocol parser while preserving the real scoped I/O error source.
use super::*;
use std::{error::Error,fmt,io};
use zero_platform_tokio::TcpRelayStream;
use zero_traits::AsyncSocket;
struct Socket<'a> {io:&'a mut TcpRelayStream,source:Option<io::Error>}
impl Socket<'_> {
    fn capture<T>(&mut self,result:io::Result<T>)->io::Result<T> {
        result.map_err(|source| {
            let kind=source.kind();self.source=Some(source);
            io::Error::new(kind,"VLESS UDP carrier I/O failed")
        })
    }
}
impl AsyncSocket for Socket<'_> {
    type Error=io::Error;
    async fn read(&mut self,bytes:&mut [u8])->io::Result<usize> {
        let result=tokio::io::AsyncReadExt::read(self.io,bytes).await;self.capture(result)
    }
    async fn write_all(&mut self,bytes:&[u8])->io::Result<()> {
        let result=async {
            tokio::io::AsyncWriteExt::write_all(self.io,bytes).await?;
            tokio::io::AsyncWriteExt::flush(self.io).await
        }.await;self.capture(result)
    }
    async fn shutdown(&mut self)->io::Result<()> {
        let result=tokio::io::AsyncWriteExt::shutdown(self.io).await;self.capture(result)
    }
}
#[derive(Debug)]
struct WireFailure {wire:zero_core::Error,source:Option<io::Error>}
impl fmt::Display for WireFailure {fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result {self.wire.fmt(f)}}
impl Error for WireFailure {
    fn source(&self)->Option<&(dyn Error+'static)> {self.source.as_ref().map(|source|source as &(dyn Error+'static))}
}
pub(super) async fn read(io:&mut TcpRelayStream,context:&OpenContext,admission:&SessionAdmission,session:Option<SessionId>)->Result<usize,ChannelError> {
    let mut socket=Socket {io,source:None};
    let response=tokio::select! {biased;
        _=admission.cancelled()=>return Err(admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
        _=context.cancel.cancelled()=>return Err(admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
        result=with_deadline(context.deadline,crate::shared::read_response_len(&mut socket))=>result.map_err(|source|admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
    };
    response.map_err(|wire| {
        let source=socket.source.take();
        let reason=if source.is_some() || matches!(wire,zero_core::Error::Io(_)) {Reason::Io} else {Reason::Protocol};
        if reason==Reason::Protocol {
            if let Some(observer)=&context.observer {observer.record(session,ConnectFact::PeerRejected);}
        }
        admission.error(session,Stage::Handshake,reason,DeliveryState::Unknown).with_source(WireFailure {wire,source})
    })
}
