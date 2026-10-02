//! Bounded raw-IP output with opaque per-flow observation provenance.
use std::sync::Arc;
use tokio::sync::mpsc;
use zero_traits::IoObserver;

#[derive(Debug)]
pub struct ObservedPacket {
    pub packet: Vec<u8>,
    pub observer: Option<Arc<dyn IoObserver>>,
}
#[derive(Clone, Debug)]
pub struct PacketSender {
    channel: Channel,
    observer: Option<Arc<dyn IoObserver>>,
}
#[derive(Clone, Debug)]
enum Channel {
    Plain(mpsc::Sender<Vec<u8>>),
    Observed(mpsc::Sender<ObservedPacket>),
}
pub enum PacketPermit {
    Plain(mpsc::OwnedPermit<Vec<u8>>),
    Observed(
        mpsc::OwnedPermit<ObservedPacket>,
        Option<Arc<dyn IoObserver>>,
    ),
}
pub struct WeakPacketSender {
    channel: WeakChannel,
    observer: Option<Arc<dyn IoObserver>>,
}
enum WeakChannel {
    Plain(mpsc::WeakSender<Vec<u8>>),
    Observed(mpsc::WeakSender<ObservedPacket>),
}
impl From<mpsc::Sender<Vec<u8>>> for PacketSender {
    fn from(sender: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            channel: Channel::Plain(sender),
            observer: None,
        }
    }
}
impl From<mpsc::Sender<ObservedPacket>> for PacketSender {
    fn from(sender: mpsc::Sender<ObservedPacket>) -> Self {
        Self {
            channel: Channel::Observed(sender),
            observer: None,
        }
    }
}
impl PacketSender {
    pub fn with_observer(mut self, observer: Option<Arc<dyn IoObserver>>) -> Self {
        self.observer = observer;
        self
    }
    pub fn observer(&self) -> Option<Arc<dyn IoObserver>> {
        self.observer.clone()
    }
    pub async fn send(&self, packet: Vec<u8>) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
        self.send_packet(ObservedPacket {
            packet,
            observer: self.observer.clone(),
        })
        .await
    }
    pub async fn send_packet(
        &self,
        packet: ObservedPacket,
    ) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
        match &self.channel {
            Channel::Plain(sender) => sender.send(packet.packet).await,
            Channel::Observed(sender) => sender
                .send(packet)
                .await
                .map_err(|e| mpsc::error::SendError(e.0.packet)),
        }
    }
    pub fn try_send(&self, packet: Vec<u8>) -> Result<(), mpsc::error::TrySendError<Vec<u8>>> {
        self.try_send_packet(ObservedPacket {
            packet,
            observer: self.observer.clone(),
        })
    }
    pub fn try_send_packet(
        &self,
        packet: ObservedPacket,
    ) -> Result<(), mpsc::error::TrySendError<Vec<u8>>> {
        match &self.channel {
            Channel::Plain(sender) => sender.try_send(packet.packet),
            Channel::Observed(sender) => sender.try_send(packet).map_err(|e| match e {
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
            Channel::Observed(sender) => sender
                .reserve_owned()
                .await
                .map(|permit| PacketPermit::Observed(permit, self.observer)),
        }
    }
    pub fn downgrade(&self) -> WeakPacketSender {
        WeakPacketSender {
            observer: self.observer.clone(),
            channel: match &self.channel {
                Channel::Plain(sender) => WeakChannel::Plain(sender.downgrade()),
                Channel::Observed(sender) => WeakChannel::Observed(sender.downgrade()),
            },
        }
    }
}
impl WeakPacketSender {
    pub fn upgrade(&self) -> Option<PacketSender> {
        Some(PacketSender {
            observer: self.observer.clone(),
            channel: match &self.channel {
                WeakChannel::Plain(sender) => Channel::Plain(sender.upgrade()?),
                WeakChannel::Observed(sender) => Channel::Observed(sender.upgrade()?),
            },
        })
    }
}
impl PacketPermit {
    pub fn send(self, packet: Vec<u8>) {
        match self {
            Self::Plain(permit) => {
                permit.send(packet);
            }
            Self::Observed(permit, observer) => {
                permit.send(ObservedPacket { packet, observer });
            }
        }
    }
}
