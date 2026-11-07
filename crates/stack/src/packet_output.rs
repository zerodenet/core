//! Bounded raw-IP output with opaque per-flow observation provenance.
#[cfg(feature = "packet-memory")]
pub mod memory;
use std::sync::Arc;
use tokio::sync::mpsc;
use zero_traits::{IoObserver, PacketBuffer};

#[derive(Debug)]
pub struct ObservedPacket {
    pub packet: PacketBuffer,
    pub observer: Option<Arc<dyn IoObserver>>,
}
#[derive(Clone, Debug)]
pub struct PacketSender {
    #[cfg(feature = "packet-memory")]
    memory: Option<memory::Binding>,
    return_mtu: Option<u16>,
    channel: Channel,
    observer: Option<Arc<dyn IoObserver>>,
}
#[derive(Clone, Debug)]
enum Channel {
    Plain(mpsc::Sender<Vec<u8>>),
    Owned(mpsc::Sender<PacketBuffer>),
    Observed(mpsc::Sender<ObservedPacket>),
}
pub enum PacketPermit {
    Plain(mpsc::OwnedPermit<Vec<u8>>),
    Owned(mpsc::OwnedPermit<PacketBuffer>),
    Observed(
        mpsc::OwnedPermit<ObservedPacket>,
        Option<Arc<dyn IoObserver>>,
    ),
}
pub struct WeakPacketSender {
    #[cfg(feature = "packet-memory")]
    memory: Option<memory::Binding>,
    return_mtu: Option<u16>,
    channel: WeakChannel,
    observer: Option<Arc<dyn IoObserver>>,
}
enum WeakChannel {
    Plain(mpsc::WeakSender<Vec<u8>>),
    Owned(mpsc::WeakSender<PacketBuffer>),
    Observed(mpsc::WeakSender<ObservedPacket>),
}
impl From<mpsc::Sender<Vec<u8>>> for PacketSender {
    fn from(sender: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            #[cfg(feature = "packet-memory")]
            memory: None,
            return_mtu: None,
            channel: Channel::Plain(sender),
            observer: None,
        }
    }
}
impl From<mpsc::Sender<ObservedPacket>> for PacketSender {
    fn from(sender: mpsc::Sender<ObservedPacket>) -> Self {
        Self {
            #[cfg(feature = "packet-memory")]
            memory: None,
            return_mtu: None,
            channel: Channel::Observed(sender),
            observer: None,
        }
    }
}
impl From<mpsc::Sender<PacketBuffer>> for PacketSender {
    fn from(sender: mpsc::Sender<PacketBuffer>) -> Self {
        Self {
            #[cfg(feature = "packet-memory")]
            memory: None,
            return_mtu: None,
            channel: Channel::Owned(sender),
            observer: None,
        }
    }
}
impl PacketSender {
    pub(crate) fn memory_changed(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> {
        #[cfg(feature = "packet-memory")]
        {
            return self.memory.as_ref().map(memory::Binding::changed);
        }
        #[cfg(not(feature = "packet-memory"))]
        None
    }
    /// Metadata from the original accepted Source; no native/role authority.
    pub fn with_return_mtu(mut self, mtu: u16) -> Self {
        self.return_mtu = Some(mtu);
        self
    }
    pub fn return_mtu(&self) -> Option<u16> {
        self.return_mtu
    }

    /// Identity of this existing bounded sink; observer wrappers grant no
    /// authority to replace another live Source session's return channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        match (&self.channel, &other.channel) {
            (Channel::Plain(a), Channel::Plain(b)) => a.same_channel(b),
            (Channel::Owned(a), Channel::Owned(b)) => a.same_channel(b),
            (Channel::Observed(a), Channel::Observed(b)) => a.same_channel(b),
            _ => false,
        }
    }
    /// Keep external storage owned until the receiver consumes or drops it.
    /// Vec-only compatibility channels convert only after capacity is reserved.
    pub async fn send_buffer(
        &self,
        packet: PacketBuffer,
    ) -> Result<(), mpsc::error::SendError<PacketBuffer>> {
        match &self.channel {
            Channel::Owned(sender) => sender.send(packet).await,
            Channel::Plain(sender) => match sender.reserve().await {
                Ok(permit) => {
                    permit.send(packet.into_vec());
                    Ok(())
                }
                Err(_) => Err(mpsc::error::SendError(packet)),
            },
            Channel::Observed(sender) => match sender.reserve().await {
                Ok(permit) => {
                    permit.send(ObservedPacket {
                        packet,
                        observer: self.observer.clone(),
                    });
                    Ok(())
                }
                Err(_) => Err(mpsc::error::SendError(packet)),
            },
        }
    }
    pub fn try_send_buffer(
        &self,
        packet: PacketBuffer,
    ) -> Result<(), mpsc::error::TrySendError<PacketBuffer>> {
        fn rejected<T>(
            error: mpsc::error::TrySendError<T>,
            packet: PacketBuffer,
        ) -> mpsc::error::TrySendError<PacketBuffer> {
            match error {
                mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(packet),
                mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(packet),
            }
        }
        match &self.channel {
            Channel::Owned(sender) => sender.try_send(packet),
            Channel::Plain(sender) => match sender.try_reserve() {
                Ok(permit) => {
                    permit.send(packet.into_vec());
                    Ok(())
                }
                Err(error) => Err(rejected(error, packet)),
            },
            Channel::Observed(sender) => match sender.try_reserve() {
                Ok(permit) => {
                    permit.send(ObservedPacket {
                        packet,
                        observer: self.observer.clone(),
                    });
                    Ok(())
                }
                Err(error) => Err(rejected(error, packet)),
            },
        }
    }
    pub fn is_closed(&self) -> bool {
        match &self.channel {
            Channel::Plain(s) => s.is_closed(),
            Channel::Owned(s) => s.is_closed(),
            Channel::Observed(s) => s.is_closed(),
        }
    }
    pub async fn closed(&self) {
        match &self.channel {
            Channel::Plain(s) => s.closed().await,
            Channel::Owned(s) => s.closed().await,
            Channel::Observed(s) => s.closed().await,
        }
    }
    pub fn with_observer(mut self, observer: Option<Arc<dyn IoObserver>>) -> Self {
        self.observer = observer;
        self
    }
    pub fn observer(&self) -> Option<Arc<dyn IoObserver>> {
        self.observer.clone()
    }
    pub async fn send(&self, packet: Vec<u8>) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
        self.send_packet(ObservedPacket {
            packet: packet.into(),
            observer: self.observer.clone(),
        })
        .await
        .map_err(|error| mpsc::error::SendError(error.0.into_vec()))
    }
    pub async fn send_packet(
        &self,
        packet: ObservedPacket,
    ) -> Result<(), mpsc::error::SendError<PacketBuffer>> {
        match &self.channel {
            Channel::Plain(sender) => match sender.reserve().await {
                Ok(permit) => {
                    permit.send(packet.packet.into_vec());
                    Ok(())
                }
                Err(_) => Err(mpsc::error::SendError(packet.packet)),
            },
            Channel::Owned(sender) => sender.send(packet.packet).await,
            Channel::Observed(sender) => sender
                .send(packet)
                .await
                .map_err(|error| mpsc::error::SendError(error.0.packet)),
        }
    }
    pub fn try_send(&self, packet: Vec<u8>) -> Result<(), mpsc::error::TrySendError<Vec<u8>>> {
        self.try_send_packet(ObservedPacket {
            packet: packet.into(),
            observer: self.observer.clone(),
        })
        .map_err(|error| match error {
            mpsc::error::TrySendError::Full(packet) => {
                mpsc::error::TrySendError::Full(packet.into_vec())
            }
            mpsc::error::TrySendError::Closed(packet) => {
                mpsc::error::TrySendError::Closed(packet.into_vec())
            }
        })
    }
    pub fn try_send_packet(
        &self,
        packet: ObservedPacket,
    ) -> Result<(), mpsc::error::TrySendError<PacketBuffer>> {
        match &self.channel {
            Channel::Plain(sender) => match sender.try_reserve() {
                Ok(permit) => {
                    permit.send(packet.packet.into_vec());
                    Ok(())
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    Err(mpsc::error::TrySendError::Full(packet.packet))
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(mpsc::error::TrySendError::Closed(packet.packet))
                }
            },
            Channel::Owned(sender) => sender.try_send(packet.packet),
            Channel::Observed(sender) => sender.try_send(packet).map_err(|error| match error {
                mpsc::error::TrySendError::Full(packet) => {
                    mpsc::error::TrySendError::Full(packet.packet)
                }
                mpsc::error::TrySendError::Closed(packet) => {
                    mpsc::error::TrySendError::Closed(packet.packet)
                }
            }),
        }
    }
    pub async fn reserve_owned(self) -> Result<PacketPermit, mpsc::error::SendError<()>> {
        match self.channel {
            Channel::Plain(sender) => sender.reserve_owned().await.map(PacketPermit::Plain),
            Channel::Owned(sender) => sender.reserve_owned().await.map(PacketPermit::Owned),
            Channel::Observed(sender) => sender
                .reserve_owned()
                .await
                .map(|permit| PacketPermit::Observed(permit, self.observer)),
        }
    }
    pub fn downgrade(&self) -> WeakPacketSender {
        WeakPacketSender {
            #[cfg(feature = "packet-memory")]
            memory: self.memory.clone(),
            return_mtu: self.return_mtu,
            observer: self.observer.clone(),
            channel: match &self.channel {
                Channel::Plain(sender) => WeakChannel::Plain(sender.downgrade()),
                Channel::Owned(sender) => WeakChannel::Owned(sender.downgrade()),
                Channel::Observed(sender) => WeakChannel::Observed(sender.downgrade()),
            },
        }
    }
}
impl WeakPacketSender {
    pub fn upgrade(&self) -> Option<PacketSender> {
        Some(PacketSender {
            #[cfg(feature = "packet-memory")]
            memory: self.memory.clone(),
            return_mtu: self.return_mtu,
            observer: self.observer.clone(),
            channel: match &self.channel {
                WeakChannel::Plain(sender) => Channel::Plain(sender.upgrade()?),
                WeakChannel::Owned(sender) => Channel::Owned(sender.upgrade()?),
                WeakChannel::Observed(sender) => Channel::Observed(sender.upgrade()?),
            },
        })
    }
}
impl PacketPermit {
    pub fn send_buffer(self, packet: PacketBuffer) {
        match self {
            Self::Plain(permit) => {
                permit.send(packet.into_vec());
            }
            Self::Owned(permit) => {
                permit.send(packet);
            }
            Self::Observed(permit, observer) => {
                permit.send(ObservedPacket { packet, observer });
            }
        }
    }
    pub fn send(self, packet: Vec<u8>) {
        match self {
            Self::Plain(permit) => {
                permit.send(packet);
            }
            Self::Owned(permit) => {
                permit.send(packet.into());
            }
            Self::Observed(permit, observer) => {
                permit.send(ObservedPacket {
                    packet: packet.into(),
                    observer,
                });
            }
        }
    }
}
