//! The actual factory, original TCP provider and same original Source Scope.
use super::*;
use base64::Engine;
#[tokio::test]
async fn encrypted_source_uses_declared_maintenance_and_keeps_native_halfclose_and_scope_receipts()
{
    timeout(Duration::from_secs(15), async {
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let decryption = format!("mlkem768x25519plus.native.600s.100-35-35.{}",b64.encode([7;32]));
        let keys = crate::encryption::EncryptionServer::new(
            crate::encryption::config::EncryptionConfig::server(&decryption).unwrap().unwrap()
        ).unwrap().public_keys();
        let client = crate::encryption::EncryptionClient::new(
            crate::encryption::config::EncryptionConfig::client(&format!(
                "mlkem768x25519plus.native.0rtt.100-35-35.{}",b64.encode(&keys[0])
            )).unwrap().unwrap()
        ).unwrap();
        let request = request_options_with_decryption(USER,None,None,None,Some(&decryption));
        let mut host = fixture::Host::with_request(request).await;
        let (raw, accepted) = host.accept().await;
        let (pending, suspended) = tokio::sync::oneshot::channel();
        let ((),()) = tokio::join!(async {
            let mut io = client.handshake(raw).await.unwrap();
            io.write_all(&header(1)).await.unwrap();
            io.write_all(b"wire").await.unwrap();
            io.flush().await.unwrap();
            let mut response = [0;2];
            io.read_exact(&mut response).await.unwrap();
            assert_eq!(response,[0,0]);
            suspended.await.unwrap();
            io.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes,b"after EOF");
        },async {
            let (request, _) = accepted.await.unwrap();
            let AcceptedRequest::Stream(request) = request else { panic!("real decrypted Stream") };
            assert_eq!(request.facts().session,SessionId(701));
            let mut stream = request.respond(ReplyOutcome::LocalChannelReady).await.unwrap().unwrap();
            let mut wire = [0;4];stream.read_exact(&mut wire).await.unwrap();assert_eq!(&wire,b"wire");
            let mut tail = Vec::new();
            {
                let mut reading = Box::pin(stream.read_to_end(&mut tail));
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(reading.as_mut(),cx).is_pending(),"actual original encrypted native read is pending before peer halfclose");
                    std::task::Poll::Ready(())
                }).await;
                pending.send(()).unwrap();
                reading.await.unwrap();
            }
            assert!(tail.is_empty());
            stream.write_all(b"after EOF").await.unwrap();
            stream.shutdown().await.unwrap();
        });
        // The actual cache Critical task and actual accepted task have distinct
        // joins. Host.close also consumes the original listener/accepted native receipts.
        host.close(2).await;
    }).await.unwrap();
}
