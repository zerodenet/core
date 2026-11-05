//! A validated wire peer uses the same original client worker owner.
use super::*;

pub(crate) struct WireStackRecipe {
    local_addresses: Vec<IpAddr>,
    mtu: u16,
    endpoint: SocketAddr,
    carrier: Arc<dyn RawIpWireCarrier>,
    tunnel: Box<dyn RawIpTunnel>,
    options: super::super::RawIpStackOptions,
}
impl WireStackRecipe {
    /// Native acquisition belongs to the original provider before this unique
    /// carrier view is transferred. This constructor starts no driver/task.
    pub(crate) fn prepare(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
        options: super::super::RawIpStackOptions,
    ) -> Result<Self, EngineError> {
        ClientUdpStack::validate_configuration(&local_addresses, mtu, options.udp_receive_queue)
            .map_err(stack_udp_error)?;
        Ok(Self {
            local_addresses,
            mtu,
            endpoint,
            carrier,
            tunnel,
            options,
        })
    }
    pub(in crate::runtime::raw_ip::device) fn start_with_workers(
        self,
        workers: Arc<super::workers::DeviceWorkers>,
    ) -> Result<Arc<SharedRawIpDevice>, EngineError> {
        SharedRawIpDevice::start_wire_stack(
            self.local_addresses,
            self.mtu,
            self.endpoint,
            self.carrier,
            self.tunnel,
            self.options,
            workers,
        )
    }
}

impl SharedRawIpDevice {
    #[cfg(test)]
    pub(crate) fn start(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        socket: TokioDatagramSocket,
        tunnel: Box<dyn RawIpTunnel>,
    ) -> Result<Arc<Self>, EngineError> {
        Self::start_with_carrier(
            local_addresses,
            mtu,
            endpoint,
            Arc::new(DirectRawIpWireCarrier::new(socket)),
            tunnel,
        )
    }

    #[cfg(test)]
    pub(crate) fn start_with_carrier(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
    ) -> Result<Arc<Self>, EngineError> {
        Self::start_metered(
            local_addresses,
            mtu,
            endpoint,
            carrier,
            tunnel,
            Default::default(),
        )
    }

    pub(crate) fn start_metered(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
        options: super::RawIpStackOptions,
    ) -> Result<Arc<Self>, EngineError> {
        let task = workers::DeviceWorkers::new();
        Self::start_wire_stack(
            local_addresses,
            mtu,
            endpoint,
            carrier,
            tunnel,
            options,
            task,
        )
    }

    fn start_wire_stack(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
        options: super::RawIpStackOptions,
        task: Arc<workers::DeviceWorkers>,
    ) -> Result<Arc<Self>, EngineError> {
        let traffic = options.traffic;
        let (outbound, raw_packets) =
            mpsc::channel::<zero_stack::packet_output::ObservedPacket>(128);
        let (forwarded_packets, forwarded_rx) = mpsc::channel(128);
        let udp = ClientUdpStack::new_with_queue(
            local_addresses.clone(),
            outbound.clone().into(),
            mtu,
            Some(Arc::new(traffic.clone())),
            options.udp_receive_queue,
        )
        .map_err(stack_udp_error)?;
        let tcp = Arc::new(
            ClientTcpStack::new_observed_with_task_owner(
                local_addresses,
                outbound.clone(),
                mtu,
                task.clone(),
            )
            .map_err(stack_tcp_error)?,
        );
        let returns = Arc::new(PacketReturns::with_drop_observer(Some(Arc::new(
            traffic.clone(),
        ))));
        let closed = Arc::new(AtomicBool::new(false));
        let health = Arc::new(Mutex::new(DeviceHealth::default()));
        let (ready_tx, ready) = watch::channel(None);
        let driver = Device {
            traffic,
            endpoint,
            carrier: carrier.clone(),
            tunnel,
            udp: udp.clone(),
            tcp: tcp.clone(),
            returns: returns.clone(),
            fragments: FragmentReassembler::new(),
            raw_packets,
            forwarded_packets: forwarded_rx,
            closed: closed.clone(),
            health: health.clone(),
        };
        task.spawn_driver(run_device(driver, ready_tx))?;
        Ok(Arc::new(Self {
            incarnation: super::next_incarnation(),
            udp,
            tcp,
            forwarded_packets,
            returns,
            closed,
            health,
            retired: AtomicBool::new(false),
            ready,
            task,
            carrier: Some(carrier),
        }))
    }
}

#[cfg(test)]
mod tests;
