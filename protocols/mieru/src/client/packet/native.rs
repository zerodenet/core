use super::*;
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, ChannelError, Impact, Reason, Stage},
    controlled_carrier::{CarrierAdoption, CarrierDatagram},
};

#[async_trait::async_trait]
impl ClientDatagramCarrier for CarrierDatagram {
    async fn send(&self, packet: &[u8]) -> io::Result<()> {
        CarrierDatagram::send(self, packet).await
    }
    async fn receive(&self, packet: &mut [u8]) -> io::Result<usize> {
        CarrierDatagram::receive(self, packet).await
    }
}
#[async_trait::async_trait]
impl ClientDatagramCarrier for zero_platform_tokio::controlled_carrier::loan::BorrowedDatagram {
    async fn send(&self, packet: &[u8]) -> io::Result<()> {
        Self::send(self, packet).await
    }
    async fn receive(&self, packet: &mut [u8]) -> io::Result<usize> {
        Self::receive(self, packet).await
    }
}
pub(crate) struct NativePacketPreparation(
    zero_platform_tokio::controlled_carrier::CarrierDriver<super::Prepared>,
);
impl ClientConnection {
    pub(crate) async fn packet_controlled(
        io: CarrierDatagram,
        adoption: CarrierAdoption,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        let ipv6 = io.peer().is_ipv6();
        Self::packet_with_driver(
            io.into_driver(),
            adoption,
            ipv6,
            username,
            password,
            options,
            lease,
        )
        .await
    }
    pub(crate) async fn packet_borrowed(
        io: zero_platform_tokio::controlled_carrier::loan::BorrowedDatagram,
        adoption: CarrierAdoption,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        let ipv6 = io.ipv6_overhead();
        Self::packet_with_driver(
            io.into_driver(),
            adoption,
            ipv6,
            username,
            password,
            options,
            lease,
        )
        .await
    }
    async fn packet_with_driver<I: ClientDatagramCarrier + 'static>(
        io: zero_platform_tokio::controlled_carrier::CarrierDriver<I>,
        adoption: CarrierAdoption,
        ipv6: bool,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
        lease: pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        let prepared = Self::prepare_packet_driver(io, ipv6, username, password, options)?;
        Self::start_packet_controlled(prepared, adoption, lease).await
    }
    pub(crate) fn prepare_packet_driver<I: ClientDatagramCarrier + 'static>(
        io: zero_platform_tokio::controlled_carrier::CarrierDriver<I>,
        ipv6: bool,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<NativePacketPreparation> {
        io.try_map(|io| super::prepare_native(Arc::new(io), ipv6, username, password, options))
            .map(NativePacketPreparation)
    }
    pub(crate) async fn start_packet_controlled(
        prepared: NativePacketPreparation,
        adoption: CarrierAdoption,
        lease: pool::managed::CarrierLease,
    ) -> io::Result<Arc<Self>> {
        let ((first, opens), prepared) = prepared
            .0
            .split(|prepared| ((prepared.first, prepared.opens), prepared.driver));
        let owner = lease.owner.clone();
        let driver = driver::ManagedDriver::new(lease.cancellation());
        let completion = driver.completion();
        let stop = driver.cancel.clone();
        let scope_stop = owner.scope_cancel.clone();
        let component = owner.instance.component;
        let registration = lease.registering();
        let (_, witness) = adoption.spawn_driver(&owner.tasks, TaskClass::Critical, prepared, |driver| async move {
            let _lease = lease;
            let _completion = completion;
            tokio::select! { biased;
                _ = stop.cancelled() => Ok(()),
                _ = scope_stop.cancelled() => Ok(()),
                result = driver => result.map_err(|source| {
                    let mut error = ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source);
                    error.component = Some(component); error
                }),
            }
        }).map_err(io::Error::other)?;
        driver.registered(witness.clone());
        driver
            .attach_original(&adoption)
            .map_err(io::Error::other)?;
        registration.registered(witness);
        let connection = Self::from_packet(opens, DriverHandle::Managed(driver));
        // The real first session was reserved before spawning this driver.
        // Taking it and publishing this same connection can now finish without
        // awaiting a second open while no cache yet retains the connection.
        *connection.first.lock().unwrap() = first;
        Ok(connection)
    }
}
