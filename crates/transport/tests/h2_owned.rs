#![cfg(feature = "h2")]
use bytes::Bytes;
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use zero_transport::{h2::connect_h2, profile::OwnedH2Profile};
fn profile() -> OwnedH2Profile {
    OwnedH2Profile {
        host: Some("original.test".into()),
        path: "/original".into(),
    }
}
async fn pending_once<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[tokio::test]
async fn caller_without_application_reads_drives_real_h2_capacity_and_end_then_reads_reverse_after_halfclose()
 {
    tokio::time::timeout(Duration::from_secs(8),async{
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let (first_tx,first_rx)=oneshot::channel();let (release_tx,release_rx)=oneshot::channel();
        let server=tokio::spawn(async move{
            let (peer,_)=listener.accept().await.unwrap();
            let mut builder=h2::server::Builder::new();builder.initial_window_size(1);
            let mut connection=builder.handshake(peer).await.unwrap();
            let (mut request,mut response)=connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(),"/original");assert_eq!(request.headers()["host"],"original.test");
            let application=async move{
                let first=request.body_mut().data().await.unwrap().unwrap();assert!(!first.is_empty());
                first_tx.send(first.len()).unwrap();release_rx.await.unwrap();
                request.body_mut().flow_control().release_capacity(first.len()).unwrap();
                let mut received=first.len();
                while let Some(data)=request.body_mut().data().await{let data=data.unwrap();assert!(data.iter().all(|byte|*byte==0x53));received+=data.len();request.body_mut().flow_control().release_capacity(data.len()).unwrap();}
                assert_eq!(received,256*1024);
                let mut send=response.send_response(http::Response::new(()),false).unwrap();
                send.send_data(Bytes::from_static(b"reverse after exact END"),true).unwrap();
            };
            let drive=async {while let Some(result)=connection.accept().await{assert!(result.is_err(),"only original application stream is expected");}};
            tokio::join!(application,drive);
        });
        let raw=TcpStream::connect(address).await.unwrap();let mut stream=connect_h2(raw,&profile(),"127.0.0.1",address.port()).await.unwrap();
        let payload=vec![0x53;256*1024];
        {
            let writing=stream.write_all(&payload);tokio::pin!(writing);
            tokio::select!{actual=first_rx=>assert!(actual.unwrap()>0),result=&mut writing=>panic!("upload completed before peer released real receive credit: {result:?}")};
            pending_once(writing.as_mut()).await;release_tx.send(()).unwrap();writing.await.unwrap();
        }
        stream.shutdown().await.unwrap();
        assert_eq!(stream.write(b"late").await.unwrap_err().kind(),std::io::ErrorKind::BrokenPipe);
        let mut reply=vec![];stream.read_to_end(&mut reply).await.unwrap();assert_eq!(reply,b"reverse after exact END");
        drop(stream);server.await.unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn split_reader_and_writer_keep_both_original_wakers_under_flow_control() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (peer, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(peer).await.unwrap();
            let (mut request, mut response) = connection.accept().await.unwrap().unwrap();
            let mut send = response
                .send_response(http::Response::new(()), false)
                .unwrap();
            let transfer = async move {
                let mut total = 0;
                while let Some(data) = request.body_mut().data().await {
                    let data = data.unwrap();
                    total += data.len();
                    request
                        .body_mut()
                        .flow_control()
                        .release_capacity(data.len())
                        .unwrap();
                }
                assert_eq!(total, 1024 * 1024);
                send.send_data(Bytes::from_static(b"both original callers woke"), true)
                    .unwrap();
            };
            let drive = async {
                while let Some(result) = connection.accept().await {
                    assert!(result.is_err());
                }
            };
            tokio::join!(transfer, drive);
        });
        let raw = TcpStream::connect(address).await.unwrap();
        let stream = connect_h2(raw, &profile(), "127.0.0.1", address.port())
            .await
            .unwrap();
        let (mut read, mut write) = tokio::io::split(stream);
        let reader = tokio::spawn(async move {
            let mut response = vec![];
            read.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"both original callers woke");
            read
        });
        let writer = tokio::spawn(async move {
            write.write_all(&vec![0x7f; 1024 * 1024]).await.unwrap();
            write.shutdown().await.unwrap();
            write
        });
        let read = reader.await.unwrap();
        let write = writer.await.unwrap();
        drop(read.unsplit(write));
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn non_success_response_is_retained_on_subsequent_reads_and_drop_closes_the_original_wire() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (peer, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(peer).await.unwrap();
            let (_, mut response) = connection.accept().await.unwrap().unwrap();
            response
                .send_response(
                    http::Response::builder().status(403).body(()).unwrap(),
                    true,
                )
                .unwrap();
            assert!(connection.accept().await.is_none());
        });
        let raw = TcpStream::connect(address).await.unwrap();
        let mut stream = connect_h2(raw, &profile(), "127.0.0.1", address.port())
            .await
            .unwrap();
        for _ in 0..2 {
            let error = stream.read(&mut [0]).await.unwrap_err();
            assert!(error.to_string().contains("403 Forbidden"));
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        }
        drop(stream);
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[path = "h2_owned/original.rs"]
mod original;
