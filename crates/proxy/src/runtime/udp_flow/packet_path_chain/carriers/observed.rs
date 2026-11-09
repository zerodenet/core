//! Observe a hop's encoded datagram boundary, below its codec.
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;
use std::{net::SocketAddr, sync::Arc};
use zero_core::Address;
use zero_engine::EngineError;
use zero_traits::IoObserver;

pub(crate) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    observer: Option<Arc<dyn IoObserver>>,
) -> Arc<dyn PacketPathCarrier> {
    match observer {
        Some(observer) => {
            observer.datagram_boundary();
            Arc::new(Observed { path, observer })
        }
        None => path,
    }
}
struct Observed {
    path: Arc<dyn PacketPathCarrier>,
    observer: Arc<dyn IoObserver>,
}
#[async_trait::async_trait]
impl PacketPathCarrier for Observed {
    fn logical_registration(
        &self,
    ) -> Option<crate::component::datagram_retirement::DatagramRegistration> {
        self.path.logical_registration()
    }

    fn has_driver_handoff(&self) -> bool {
        self.path.has_driver_handoff()
    }
    fn prepare_codec_send(
        &self,
        lengths: &[usize],
    ) -> Result<
        zero_platform_tokio::contract::component::PacketDriverSendBuffers,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.path.prepare_codec_send(lengths)
    }
    fn codec_transmit_lengths(
        &self,
        target: &Address,
        port: u16,
        len: usize,
        lengths: &mut Vec<usize>,
    ) -> Result<(), EngineError> {
        self.path.codec_transmit_lengths(target, port, len, lengths)
    }
    async fn send_codec_buffered(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [zero_traits::PacketBuffer],
        copy: &mut zero_platform_tokio::contract::component::PacketDriverSendCopy,
    ) -> Result<(), EngineError> {
        let result = self
            .path
            .send_codec_buffered(target, port, payload, buffers, copy)
            .await;
        match &result {
            Ok(()) => self.observer.sent_datagram(payload.len()),
            Err(_) => self.observer.error(),
        }
        result
    }
    fn request_driver_retirement(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.request_driver_retirement(driver)
    }
    fn confirm_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.confirm_driver_handoff(driver)
    }
    async fn retire_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        deadline: tokio::time::Instant,
    ) -> Result<
        zero_platform_tokio::contract::lifecycle::CloseReport,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.path.retire_driver_handoff(driver, deadline).await
    }
    fn native_owner(
        &self,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        self.path.native_owner()
    }

    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let result = self.path.send_to(target, port, payload).await;
        match &result {
            Ok(()) => self.observer.sent_datagram(payload.len()),
            Err(_) => self.observer.error(),
        }
        result
    }
    async fn recv_from(&self, output: &mut [u8]) -> Result<usize, EngineError> {
        let result = self.path.recv_from(output).await;
        match &result {
            Ok(size) => self.observer.received_datagram(*size),
            Err(_) => self.observer.error(),
        }
        result
    }
    async fn recv_from_with_source(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        let result = self.path.recv_from_with_source(output).await;
        match &result {
            Ok((size, _)) => self.observer.received_datagram(*size),
            Err(_) => self.observer.error(),
        }
        result
    }
}
