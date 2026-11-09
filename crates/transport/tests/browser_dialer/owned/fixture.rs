use super::*;
use zero_core::{
    component::{ComponentId, ComponentInstanceId, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    controlled_carrier::{CarrierBufferLimits, CarrierOriginalTotals},
    AcceptedTaskFailures, ControlledTcpListenerOwner,
};
use zero_tun::packet_memory::PacketReturnTotals;

pub(super) fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub(super) struct Provider {
    pub(super) native: tokio::sync::Mutex<ControlledTcpListenerOwner>,
    pub(super) totals: Arc<CarrierOriginalTotals>,
    pub(super) packet: Arc<PacketReturnTotals>,
    // The native owner and every actual caller share this one paid receiver.
    pub(super) faults: AcceptedTaskFailures,
    destination: Destination,
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, ChannelError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Provider {
    fn packet_driver_metadata(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketDriverMetadataView, ChannelError>
    {
        Ok(self.packet.driver_metadata_view())
    }
    fn carrier_buffer_budget(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierBufferBudget> {
        Some(self.totals.buffer_budget())
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async move {
            assert_eq!(
                address, self.destination,
                "the same actual declaration, not another listener"
            );
            self.native
                .lock()
                .await
                .bind_listener("127.0.0.1:0".parse().unwrap())
        })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        denied()
    }
    fn open_device(
        &self,
        _: Arc<str>,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::packet::BoxPacketSession, ChannelError>>
    {
        denied()
    }
}
pub(super) struct Fixture {
    pub(super) source: ScopedBrowserDialer,
    pub(super) scope: HostedScope,
    pub(super) provider: Arc<Provider>,
}
impl Fixture {
    pub(super) async fn start() -> Self {
        let options = BrowserDialerOptions {
            idle_capacity: 1,
            task_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let address = Destination::new(Address::Ipv4([127, 0, 0, 1]), 0);
        let declaration = ScopedBrowserDialer::declaration(address.clone(), &options).unwrap();
        // Nine accepted originals plus the one real accept/cleanup driver.
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(n(10)));
        let packet = Arc::new(PacketReturnTotals::default());
        let totals = CarrierOriginalTotals::new(n(2)); // real listener + one real accepted socket
        totals
            .install_buffer_limits_after_retirement(CarrierBufferLimits {
                max_queue_packets: 2,
                total_max_bytes: 2 * 1024 * 1024,
            })
            .unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(701),
            generation: ResourceGeneration(1),
        };
        let faults = AcceptedTaskFailures::prepare(
            instance,
            &scope.registrar(),
            &packet.driver_metadata_view(),
        )
        .unwrap();
        let mut native = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            n(1),
            scope.registrar().cancellation(),
            n(9),
            n(1),
            &scope.registrar(),
        )
        .unwrap();
        native.bind_task_failure_receiver(&faults).unwrap();
        native
            .bind_original_totals(instance, &scope.registrar(), &totals)
            .unwrap();
        let provider = Arc::new(Provider {
            native: tokio::sync::Mutex::new(native),
            totals,
            packet,
            faults,
            destination: address.clone(),
        });
        let mut source = ScopedBrowserDialer::prepare(
            address,
            options,
            provider.clone(),
            &scope.registrar(),
            scope.registrar().cancellation(),
        )
        .unwrap();
        scope.enable_registration().unwrap();
        source.start(deadline()).await.unwrap();
        Self {
            source,
            scope,
            provider,
        }
    }
    pub(super) async fn finish(&mut self) {
        self.source.revoke(CloseReason::Shutdown);
        self.scope.begin_shutdown();
        while let Some(receipt) = timeout(Duration::from_secs(1), self.scope.next_receipt())
            .await
            .unwrap()
        {
            receipt.result().unwrap();
            self.scope.acknowledge(&receipt).unwrap();
        }
        assert!(self.scope.is_drained());
        assert!(self
            .source
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(self
            .provider
            .native
            .lock()
            .await
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(self.provider.native.lock().await.confirmed_close_count(), 1);
        assert_eq!(self.provider.totals.snapshot().charged, 0);
        assert!(self.provider.faults.is_empty());
    }
}
pub(super) async fn drive<T>(scope: &mut HostedScope, future: impl Future<Output = T>) -> T {
    tokio::pin!(future);
    loop {
        tokio::select! { biased;
            result = &mut future => return result,
            receipt = scope.next_receipt() => {
                let receipt = receipt.expect("live original accept driver");
                receipt.result().unwrap(); scope.acknowledge(&receipt).unwrap();
            }
        }
    }
}
pub(super) async fn browser(
    source: &ScopedBrowserDialer,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let address = source.local_addr().unwrap();
    let mut page = tokio::net::TcpStream::connect(address).await.unwrap();
    page.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut contents = String::new();
    page.read_to_string(&mut contents).await.unwrap();
    let marker = "/websocket?token=";
    let start = contents.find(marker).unwrap() + marker.len();
    let token = &contents[start..contents[start..].find('"').unwrap() + start];
    let mut request = format!("ws://{address}{marker}{token}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("origin", format!("http://{address}").parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}
