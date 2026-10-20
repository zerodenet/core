//! Stack validation and socket allocation; no protocol or runtime policy.
use super::*;

impl ClientUdpStack {
    pub fn new(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
        mtu: u16,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, None)
    }
    pub fn new_observed(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, None)
    }
    pub fn new_observed_with_drops(
        local_addresses: Vec<IpAddr>,
        outbound: mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, drop_observer)
    }
    fn new_with_output(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_queue(
            local_addresses,
            outbound,
            mtu,
            drop_observer,
            Default::default(),
        )
    }
    /// Explicit neutral queue policy. Defaults preserve existing slot-only admission.
    pub fn new_with_queue(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
        receive_queue: ClientUdpReceiveQueueConfig,
    ) -> Result<Self, ClientUdpStackError> {
        Self::validate_configuration(&local_addresses, mtu, receive_queue)?;
        Ok(Self {
            inner: Arc::new(Inner {
                local_addresses,
                outbound,
                mtu: usize::from(mtu),
                next_fragment_id: AtomicU32::new(1),
                drop_observer,
                receive_queue,
                receive_budget: receive_queue.max_stack_payload_bytes.map(ByteBudget::new),
                sockets: Mutex::new(HashMap::new()),
                fragments: Mutex::new(FragmentReassembler::new()),
            }),
        })
    }

    /// Validate a cold stack recipe without constructing channels, state or I/O.
    pub fn validate_configuration(
        local_addresses: &[IpAddr],
        mtu: u16,
        receive_queue: ClientUdpReceiveQueueConfig,
    ) -> Result<(), ClientUdpStackError> {
        receive_queue.validate()?;
        if local_addresses.is_empty() {
            return Err(ClientUdpStackError::MissingLocalAddress);
        }
        if local_addresses
            .iter()
            .enumerate()
            .any(|(index, address)| local_addresses[..index].contains(address))
        {
            return Err(ClientUdpStackError::DuplicateLocalAddress);
        }
        if mtu < 68 || (local_addresses.iter().any(IpAddr::is_ipv6) && mtu < 1_280) {
            return Err(ClientUdpStackError::InvalidMtu);
        }
        Ok(())
    }

    /// Allocate a bounded receive queue and a randomized ephemeral source port.
    pub fn bind(&self, local: IpAddr) -> Result<ClientUdpSocket, ClientUdpStackError> {
        self.bind_observed(local, None)
    }

    pub fn bind_observed(
        &self,
        local: IpAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<ClientUdpSocket, ClientUdpStackError> {
        if !self.inner.local_addresses.contains(&local) {
            return Err(ClientUdpStackError::UnknownLocalAddress);
        }
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        if sockets.len() >= MAX_SOCKETS {
            return Err(ClientUdpStackError::SocketLimit);
        }
        let start = usize::from(rand::random_range(0..EPHEMERAL_PORT_COUNT as u16));
        for offset in 0..EPHEMERAL_PORT_COUNT {
            let index = (start + offset) % EPHEMERAL_PORT_COUNT;
            let endpoint = Endpoint {
                ip: local,
                port: FIRST_EPHEMERAL_PORT + index as u16,
            };
            if sockets.contains_key(&endpoint) {
                continue;
            }
            let (sender, receiver) = mpsc::channel(self.inner.receive_queue.capacity);
            let budget = QueueBudget::new(
                self.inner.receive_queue.max_payload_bytes,
                self.inner.receive_budget.clone(),
            );
            sockets.insert(
                endpoint,
                SocketEntry {
                    sender,
                    budget,
                    last_destination: None,
                    path_mtu: None,
                    observer: observer.clone(),
                },
            );
            return Ok(ClientUdpSocket {
                inner: Arc::clone(&self.inner),
                endpoint,
                receiver,
                observer,
            });
        }
        Err(ClientUdpStackError::SocketLimit)
    }
}
