//! Actual final logical controller, strongly installed before any view escapes.
use super::*;
impl PacketDriverCallGrant {
    /// Single-install by value. The same record already owns the grant, and
    /// retains the controller before its views escape or any validation awaits.
    pub fn install_datagram_loan(
        mut self,
        loan: DatagramLoan,
        association: AssociationId,
    ) -> Result<(BoxDatagramSession, PacketDriverDatagramHandoff), ChannelError> {
        #[cfg(feature = "native-packet-origin")]
        let mut metadata = self
            .record
            .installed_metadata
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        let (tx, rx, limits, owner) = loan.into_packet_driver_parts();
        let origin = owner.original().clone();
        let value = Installed {
            control: AsyncMutex::new(owner),
            origin,
            association,
            receiver: OnceLock::new(),
            #[cfg(feature = "native-packet-origin")]
            memory: OnceLock::new(),
            complete: false.into(),
        };
        #[cfg(feature = "native-packet-origin")]
        let installed_metadata = metadata
            .split(InstalledRef::layout().map_err(memory::memory_error)?)
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let installed = InstalledRef::from_prepaid(value, installed_metadata)
            .unwrap_or_else(|_| unreachable!("the same exact prepaid final-controller layout"));
        #[cfg(not(feature = "native-packet-origin"))]
        let installed = Arc::new(value);
        self.record
            .installed
            .set(installed.clone())
            .unwrap_or_else(|_| unreachable!("single consuming install"));
        let control = control::StoredControl {
            record: self.record.clone(),
            installed: installed.clone(),
        };
        #[cfg(feature = "native-packet-origin")]
        let control =
            crate::contract::lifecycle::BoxChannelControl::from_prepaid(control, metadata)
                .unwrap_or_else(|_| unreachable!("same exact prepaid StoredControl layout"));
        #[cfg(not(feature = "native-packet-origin"))]
        let control: crate::contract::lifecycle::BoxChannelControl = Box::new(control).into();
        let session = crate::contract::datagram::DatagramSession {
            tx,
            rx,
            limits,
            control,
        };
        let handoff = PacketDriverDatagramHandoff {
            record: self.record.clone(),
            installed,
        };
        self.installed = true;
        Ok((session, handoff))
    }
}
