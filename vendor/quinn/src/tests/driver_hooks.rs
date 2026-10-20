use std::{future::Future, pin::Pin, sync::Mutex};

use super::*;
use crate::{AsyncTimer, AsyncUdpSocket, DriverExit, DriverFuture, DriverOrigin};

type RecordedDriver = (DriverOrigin, tokio::task::JoinHandle<DriverExit>);

#[derive(Debug, Default)]
struct RecordingRuntime {
    drivers: Mutex<Vec<RecordedDriver>>,
}

impl crate::Runtime for RecordingRuntime {
    fn new_timer(&self, instant: Instant) -> Pin<Box<dyn AsyncTimer>> {
        crate::Runtime::new_timer(&TokioRuntime, instant)
    }

    fn spawn(&self, _future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        panic!("an actual Quinn driver bypassed the typed creation hook");
    }

    fn spawn_driver(&self, origin: DriverOrigin, future: DriverFuture) {
        self.drivers
            .lock()
            .unwrap()
            .push((origin, tokio::spawn(future)));
    }

    fn wrap_udp_socket(&self, socket: UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        crate::Runtime::wrap_udp_socket(&TokioRuntime, socket)
    }

    fn now(&self) -> Instant {
        crate::Runtime::now(&TokioRuntime)
    }
}

#[tokio::test]
async fn real_endpoint_and_connections_preserve_driver_origin_and_close_reason() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let factory = EndpointFactory::new();
        let key = PrivateKeyDer::Pkcs8(factory.cert.signing_key.serialize_der().into());
        let server_config =
            crate::ServerConfig::with_single_cert(vec![factory.cert.cert.der().clone()], key)
                .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(factory.cert.cert.der().clone()).unwrap();
        let runtime = Arc::new(RecordingRuntime::default());
        let mut endpoint = Endpoint::new(
            factory.endpoint_config,
            Some(server_config),
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
            runtime.clone(),
        )
        .unwrap();
        endpoint.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let (client, server) = tokio::join!(
            endpoint
                .connect(endpoint.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { endpoint.accept().await.unwrap().await },
        );
        let client = client.unwrap();
        let server = server.unwrap();
        let (sent, received) = tokio::join!(
            async {
                let mut stream = client.open_uni().await.unwrap();
                stream.write_all(b"actual driver hook").await.unwrap();
                stream.finish().unwrap();
            },
            async {
                server
                    .accept_uni()
                    .await
                    .unwrap()
                    .read_to_end(1024)
                    .await
                    .unwrap()
            },
        );
        assert_eq!(sent, ());
        assert_eq!(received, b"actual driver hook");
        client.close(7u32.into(), b"actual caller close");
        let cause = server.closed().await;
        assert!(matches!(
            cause,
            crate::ConnectionError::ApplicationClosed(_)
        ));
        drop(client);
        drop(server);
        endpoint.wait_idle().await;
        drop(endpoint);
        // These are the actual spawned tasks, not body-completion observations.
        let drivers = std::mem::take(&mut *runtime.drivers.lock().unwrap());
        assert_eq!(drivers.len(), 3);
        let mut endpoint_count = 0;
        let mut local_count = 0;
        let mut peer_count = 0;
        for (origin, task) in drivers {
            match (origin, task.await.unwrap()) {
                (DriverOrigin::Endpoint, DriverExit::Endpoint(result)) => {
                    result.unwrap();
                    endpoint_count += 1;
                }
                (
                    DriverOrigin::Connection,
                    DriverExit::Connection {
                        result,
                        close_reason,
                    },
                ) => {
                    result.unwrap();
                    match close_reason.unwrap() {
                        crate::ConnectionError::LocallyClosed => local_count += 1,
                        crate::ConnectionError::ApplicationClosed(error) => {
                            assert_eq!(error.error_code, 7u32.into());
                            assert_eq!(error.reason.as_ref(), b"actual caller close");
                            peer_count += 1;
                        }
                        error => panic!("unexpected original close reason: {error:?}"),
                    }
                }
                (origin, exit) => panic!("driver creation origin mismatch: {origin:?}, {exit:?}"),
            }
        }
        assert_eq!((endpoint_count, local_count, peer_count), (1, 1, 1));
    })
    .await
    .unwrap();
}
