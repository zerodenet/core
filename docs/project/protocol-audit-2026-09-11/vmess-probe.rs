use std::{io, pin::Pin, sync::{Arc,Mutex}, task::{Context,Poll}};
use tokio::io::{AsyncRead,AsyncWrite,ReadBuf};
use zero_traits::AsyncSocket;
use zero_core::{Address,Network,ProtocolType,Session};
struct Socket { input: io::Cursor<Vec<u8>>, output: Arc<Mutex<Vec<u8>>> }
impl Socket { fn new(data: Vec<u8>) -> Self { Self { input: io::Cursor::new(data), output: Default::default() } } }
impl AsyncSocket for Socket {
 type Error=io::Error;
 async fn read(&mut self,b:&mut [u8])->io::Result<usize>{io::Read::read(&mut self.input,b)}
 async fn write_all(&mut self,b:&[u8])->io::Result<()>{self.output.lock().unwrap().extend_from_slice(b);Ok(())}
 async fn shutdown(&mut self)->io::Result<()>{Ok(())}
}
impl AsyncRead for Socket { fn poll_read(mut self:Pin<&mut Self>,_:&mut Context<'_>,b:&mut ReadBuf<'_>)->Poll<io::Result<()>>{let mut bytes=vec![0;b.remaining()];let n=io::Read::read(&mut self.input,&mut bytes)?;b.put_slice(&bytes[..n]);Poll::Ready(Ok(()))} }
impl AsyncWrite for Socket {
 fn poll_write(self:Pin<&mut Self>,_:&mut Context<'_>,b:&[u8])->Poll<io::Result<usize>>{self.output.lock().unwrap().extend_from_slice(b);Poll::Ready(Ok(b.len()))}
 fn poll_flush(self:Pin<&mut Self>,_:&mut Context<'_>)->Poll<io::Result<()>>{Poll::Ready(Ok(()))}
 fn poll_shutdown(self:Pin<&mut Self>,_:&mut Context<'_>)->Poll<io::Result<()>>{Poll::Ready(Ok(()))}
}
fn target()->Session{Session::new(0,Address::Domain("example.com".into()),443,Network::Tcp,ProtocolType::new("audit"))}

#[tokio::test]
async fn audit_identical_vmess_handshake_is_accepted_twice(){
 let id="11111111-2222-3333-4444-555555555555";
 let socket=Socket::new(vec![]);let output=socket.output.clone();
 let request=vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(id,"aes-128-gcm",None).unwrap();
 let (_stream,_)=request.establish_tcp_outbound_stream(socket,&target()).await.unwrap();
 let wire=output.lock().unwrap().clone();
 let profile=vmess::inbound::VmessInboundProfile::from_config_users([(id.to_owned(),"aes-128-gcm".to_owned(),None::<String>,None::<u64>,None::<u64>,None::<u32>,None::<u64>,None::<u64>)]).unwrap();
 for i in 1..=2 {let accepted=profile.accept_tcp_stream(vmess::inbound::VmessInbound,Socket::new(wire.clone())).await;assert!(accepted.is_ok());println!("VMess identical captured handshake attempt {i}: ACCEPTED");}
}

#[allow(dead_code)]
#[path="../src/validation.rs"] mod audit_validation;
use audit_validation::VmessCipher;
#[allow(dead_code)]
#[path="../src/crypto.rs"] mod wire_crypto;
#[tokio::test]
async fn audit_vmess_stale_authid_is_accepted(){
 let id="11111111-2222-3333-4444-555555555555";
 let socket=Socket::new(vec![]);let output=socket.output.clone();
 let request=vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(id,"aes-128-gcm",None).unwrap();
 let (_stream,_)=request.establish_tcp_outbound_stream(socket,&target()).await.unwrap();
 let wire=output.lock().unwrap().clone();
 let key=wire_crypto::derive_xray_cmd_key(&vmess::parse_uuid(id).unwrap());
 let auth: [u8;16]=wire[..16].try_into().unwrap();let nonce:[u8;8]=wire[34..42].try_into().unwrap();
 let body=wire_crypto::open_xray_aead_header_payload(&key,&auth,&nonce,&wire[42..]).unwrap();
 let stale=wire_crypto::create_xray_auth_id(&key,1).unwrap();
 let wire=wire_crypto::seal_xray_aead_header(&key,&stale,&body).unwrap();
 let profile=vmess::inbound::VmessInboundProfile::from_config_users([(id.to_owned(),"aes-128-gcm".to_owned(),None::<String>,None::<u64>,None::<u64>,None::<u32>,None::<u64>,None::<u64>)]).unwrap();
 assert!(profile.accept_tcp_stream(vmess::inbound::VmessInbound,Socket::new(wire)).await.is_ok());
 println!("VMess AuthID timestamp=1 (1970): ACCEPTED");
}
