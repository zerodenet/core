use super::*;
use crate::grpc::{prepared::PreparedConnection, prepared_stream::PreparedStream};

#[tokio::test]
async fn original_stream_workers_run_inline_with_flow_control_half_close_and_grpc_trailers() {
    timeout(Duration::from_secs(5),async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let profile=OwnedGrpcProfile {service_names:vec!["inline/Tun".into()],multi_mode:true,..Default::default()};
        let server_profile=profile.clone();let (finish,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (io,_)=listener.accept().await.unwrap();let incoming=accept_grpc_connection(io,&server_profile).unwrap();
            let mut stream=incoming.accept().await.unwrap();let mut data=Vec::new();stream.read_to_end(&mut data).await.unwrap();
            assert_eq!(data,vec![0x53;196_613]);
            AsyncWriteExt::write_all(&mut stream,&data).await.unwrap();AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            finished.await.unwrap();incoming.close();
        });
        let io=tokio::net::TcpStream::connect(address).await.unwrap();
        let PreparedConnection {sender,driver,active:_}=crate::grpc::prepared::prepare_connection(io,profile.clone(),None).await.unwrap();
        let logical=async move {
            let mut sender=sender.ready().await.unwrap();
            let (response,send)=sender.send_request(options::request(&profile,"localhost").unwrap(),false).unwrap();
            let PreparedStream{mut stream,read,write}=crate::grpc::prepared_stream::prepare_stream(send,response,true);
            let io=async move {
                AsyncWriteExt::write_all(&mut stream,&vec![0x53;196_613]).await.unwrap();
                AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                // Local END_STREAM retains the real reverse direction.
                let mut data=Vec::new();stream.read_to_end(&mut data).await.unwrap();assert_eq!(data,vec![0x53;196_613]);
            };
            tokio::join!(read,write,io);
            drop(sender);
        };
        tokio::pin!(driver);
        tokio::select! {biased;
            _=logical=>{},
            _=driver.as_mut()=>panic!("connection driver exited before the inline stream workers completed"),
        }
        // This seam test owns no controlled fd and makes no close/join claim.
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn inline_writer_retains_the_original_h2_reset_error_instead_of_a_display_string() {
    timeout(Duration::from_secs(5),async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let (finish,mut finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (io,_)=listener.accept().await.unwrap();let mut connection=h2::server::handshake(io).await.unwrap();
            let (_request,mut respond)=connection.accept().await.unwrap().unwrap();respond.send_reset(h2::Reason::REFUSED_STREAM);
            tokio::select! {
                _=&mut finished=>{},
                _=connection.accept()=>panic!("test holds the actual connection while its reset is observed"),
            }
        });
        let profile=OwnedGrpcProfile {service_names:vec!["source/Tun".into()],..Default::default()};
        let io=tokio::net::TcpStream::connect(address).await.unwrap();
        let PreparedConnection{sender,driver,active:_}=crate::grpc::prepared::prepare_connection(io,profile.clone(),None).await.unwrap();
        let logical=async move {
            let mut sender=sender.ready().await.unwrap();let (response,mut send)=sender.send_request(options::request(&profile,"localhost").unwrap(),false).unwrap();
            let reset=futures_util::future::poll_fn(|cx|send.poll_reset(cx)).await.unwrap();assert_eq!(reset,h2::Reason::REFUSED_STREAM);
            let PreparedStream{mut stream,read,write}=crate::grpc::prepared_stream::prepare_stream(send,response,false);
            let io=async move {
                AsyncWriteExt::write_all(&mut stream,b"retained-source").await.unwrap();
                let error=AsyncWriteExt::flush(&mut stream).await.unwrap_err();
                assert!(original_h2(&error).is_some_and(|original|original.reason()==Some(h2::Reason::REFUSED_STREAM)),"actual h2::Error object is retained: {error:?}");
            };
            tokio::join!(read,write,io);drop(sender);
        };
        tokio::pin!(driver);
        tokio::select! {biased;_=logical=>{},_=driver.as_mut()=>panic!("connection must stay live across this logical reset")}
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}
fn original_h2<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a h2::Error> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(original) = error.downcast_ref::<h2::Error>() {
            return Some(original);
        }
        if let Some(original) = error
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
            .and_then(|inner| inner.downcast_ref::<h2::Error>())
        {
            return Some(original);
        }
        current = error.source();
    }
    None
}
