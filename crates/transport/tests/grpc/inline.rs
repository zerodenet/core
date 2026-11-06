//! The per-connection caller advances original H2 and both hunk futures itself.
use super::*;
use crate::profile::OwnedGrpcProfile;
use tokio::time::{timeout, Duration};
#[tokio::test]
async fn inline_single_and_multi_clients_drain_upload_without_application_reads_and_keep_reverse_half_close(
) {
    timeout(Duration::from_secs(8), async {
        for multi_mode in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let profile = OwnedGrpcProfile {
                service_names: vec!["inline/Tun".into()],
                multi_mode,
                ..Default::default()
            };
            let server_profile = profile.clone();
            let (done, finished) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let incoming = accept_grpc_connection(io, &server_profile).unwrap();
                let mut io = incoming.accept().await.unwrap();
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, vec![0x79; 2 * 1024 * 1024 + 13]);
                AsyncWriteExt::write_all(&mut io, &body).await.unwrap();
                AsyncWriteExt::shutdown(&mut io).await.unwrap();
                finished.await.unwrap();
                incoming.close();
            });
            let raw = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut io = connect_grpc_inline_with_settings(raw, &profile, "localhost", None)
                .await
                .unwrap();
            assert!(io.inline.is_some() && io.driver.is_none() && io.scoped.is_none());
            // More than both the original queue and H2 flow-control windows;
            // no client task or application read drives this actual upload.
            AsyncWriteExt::write_all(&mut io, &vec![0x79; 2 * 1024 * 1024 + 13])
                .await
                .unwrap();
            AsyncWriteExt::shutdown(&mut io).await.unwrap();
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, vec![0x79; 2 * 1024 * 1024 + 13]);
            done.send(()).unwrap();
            drop(io);
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
    // This transport test supplies ordinary TCP and asserts no controlled
    // native-close or Scope receipt from body completion. Protocol owners do so.
}

#[derive(Default)]
struct Wakes(std::sync::atomic::AtomicUsize, tokio::sync::Notify);
impl futures_util::task::ArcWake for Wakes {
    fn wake_by_ref(this: &std::sync::Arc<Self>) {
        this.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        this.1.notify_one();
    }
}
#[tokio::test]
async fn split_real_capacity_wakes_the_writer_after_a_distinct_pending_reader_is_cancelled() {
    use futures_util::task::waker_ref;
    use std::{
        future::Future,
        sync::{atomic::Ordering, Arc},
        task::Context,
    };
    timeout(Duration::from_secs(8),async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let profile=OwnedGrpcProfile{service_names:vec!["split/Tun".into()],..Default::default()};let server_profile=profile.clone();
        let(full,filled)=tokio::sync::oneshot::channel();let(release,released)=tokio::sync::oneshot::channel();let(done,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let(io,_)=listener.accept().await.unwrap();let incoming=accept_grpc_connection(io,&server_profile).unwrap();let mut io=incoming.accept().await.unwrap();
            // The actual bounded peer hunk queue has consumed every credit.
            timeout(Duration::from_secs(3),async {while io.read_rx.len()<64 {tokio::task::yield_now().await;}}).await.unwrap();
            full.send(()).unwrap();released.await.unwrap();let mut received=Vec::new();io.read_to_end(&mut received).await.unwrap();
            assert_eq!(received,vec![0x63;8*1024*1024+7]);AsyncWriteExt::write_all(&mut io,b"done").await.unwrap();AsyncWriteExt::shutdown(&mut io).await.unwrap();
            finished.await.unwrap();incoming.close();
        });
        let raw=tokio::net::TcpStream::connect(address).await.unwrap();let stream=connect_grpc_inline_with_settings(raw,&profile,"localhost",None).await.unwrap();
        let(mut reader,mut writer)=tokio::io::split(stream);let payload=vec![0x63;8*1024*1024+7];
        let mut writing=Box::pin(AsyncWriteExt::write_all(&mut writer,&payload));
        tokio::select! {result=&mut writing=>panic!("peer capacity was held, upload finished early: {result:?}"),_=filled=>{}}
        let writer_wakes=Arc::new(Wakes::default());let writer_waker=waker_ref(&writer_wakes);
        assert!(writing.as_mut().poll(&mut Context::from_waker(&writer_waker)).is_pending());
        let before=writer_wakes.0.load(Ordering::SeqCst);
        let reader_wakes=Arc::new(Wakes::default());let reader_waker=waker_ref(&reader_wakes);let mut byte=[0];
        {let reading=AsyncReadExt::read(&mut reader,&mut byte);tokio::pin!(reading);assert!(reading.as_mut().poll(&mut Context::from_waker(&reader_waker)).is_pending());}
        // This read's registered task no longer polls. Only the actual peer's
        // released H2 credit can unblock the still-held upload. Do not repoll
        // writing until its previously registered real waker has been called.
        release.send(()).unwrap();
        timeout(Duration::from_secs(3),async {
            loop {let changed=writer_wakes.1.notified();tokio::pin!(changed);changed.as_mut().enable();
                if writer_wakes.0.load(Ordering::SeqCst)>before {break;}changed.await;}
        }).await.expect("original capacity must wake the writer independently of the cancelled reader");
        writing.await.unwrap();AsyncWriteExt::shutdown(&mut writer).await.unwrap();let mut response=Vec::new();reader.read_to_end(&mut response).await.unwrap();assert_eq!(response,b"done");
        drop(reader.unsplit(writer));done.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}
