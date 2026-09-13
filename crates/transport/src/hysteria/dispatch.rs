use super::*;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
pub(super) type Web = Result<(quinn::SendStream, quinn::RecvStream, Bytes), quinn::ConnectionError>;
pub(super) async fn run(
    connection: quinn::Connection,
    authenticated: Arc<AtomicBool>,
    web: mpsc::Sender<Web>,
    data: mpsc::Sender<HysteriaStream>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _=data.closed()=>break,
            pair=connection.accept_bi(),if tasks.len()<128=>{
                let (mut send,mut recv)=match pair{Ok(pair)=>pair,Err(error)=>{let _=web.try_send(Err(error));break}};
                let authenticated=authenticated.clone();let web=web.clone();let data=data.clone();let connection=connection.clone();
                tasks.spawn(async move {
                    let mut bytes=[0;8];
                    let read=async {
                        recv.read_exact(&mut bytes[..1]).await?;let length=1usize<<(bytes[0]>>6);
                        if length>1{recv.read_exact(&mut bytes[1..length]).await?;}Ok::<_,quinn::ReadExactError>(length)
                    };
                    let length=match tokio::time::timeout(std::time::Duration::from_secs(10),read).await{Ok(Ok(length))=>length,_=>return};
                    let mut frame=u64::from(bytes[0]&63);for byte in &bytes[1..length]{frame=(frame<<8)|u64::from(*byte);}
                    if frame==0x401 {
                        if authenticated.load(Ordering::Acquire){let _=data.try_send(HysteriaStream::new(send,recv,&connection,None));}
                        else{let _=recv.stop(0x10bu32.into());let _=send.reset(0x10bu32.into());}
                    }else{let _=web.try_send(Ok((send,recv,Bytes::copy_from_slice(&bytes[..length]))));}
                });
            }
            _=tasks.join_next(),if !tasks.is_empty()=>{}
            _=connection.closed()=>break,
        }
    }
}
