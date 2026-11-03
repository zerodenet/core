use super::*;
use crate::runtime::{
    udp_delivery::*, udp_dispatch::UdpDispatch, udp_ingress::UdpIngressRuntime,
    udp_socket::DirectUdpResponseSource,
};
use std::{future::poll_fn, pin::Pin};
use zero_platform_tokio::contract::{
    datagram::DatagramLimits,
    inbound::{DynInboundDatagramTx, InboundDatagramResponse},
    lifecycle::DynChannelControl,
    DeliveryState,
};
type Tx = Pin<Box<DynInboundDatagramTx>>;

async fn send(
    tx: &mut Tx,
    control: &mut dyn DynChannelControl,
    limits: &DatagramLimits,
    source: &zero_core::Address,
    port: u16,
    payload: &[u8],
    client_session_id: Option<u64>,
) -> Result<usize, EngineError> {
    let frame = InboundDatagramResponse {
        source: zero_core::request::Destination::new(source.clone(), port),
        payload,
        client_session_id,
    };
    if !frame.validate(limits) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "inbound Datagram response exceeds its channel limits",
        )
        .into());
    }
    struct Pending<'a> {
        control: &'a mut dyn DynChannelControl,
        armed: bool,
    }
    impl Drop for Pending<'_> {
        fn drop(&mut self) {
            if self.armed {
                self.control.cancel(CloseReason::DeliveryUnknown);
            }
        }
    }
    let mut pending = Pending {
        control,
        armed: false,
    };
    let result = poll_fn(|cx| {
        pending.armed = true;
        let result = tx.as_mut().poll_send(cx, &frame);
        if result.is_ready() {
            pending.armed = false;
        }
        result
    })
    .await;
    let receipt = match result {
        Ok(receipt)
            if receipt.payload.accepted_bytes == payload.len()
                && receipt.payload.delivery == DeliveryState::LocallyAccepted =>
        {
            receipt
        }
        Ok(_) => {
            pending.control.cancel(CloseReason::DeliveryUnknown);
            return Err(std::io::Error::other(
                "inbound Datagram send returned a partial/invalid receipt",
            )
            .into());
        }
        Err(failure) => {
            if failure.delivery == DeliveryState::Unknown {
                pending.control.cancel(CloseReason::DeliveryUnknown);
            }
            return Err(std::io::Error::other(failure.error).into());
        }
    };
    Ok(receipt.protocol_bytes)
}
pub(super) async fn direct(
    runtime: &UdpIngressRuntime,
    dispatch: &UdpDispatch,
    tx: &mut Tx,
    control: &mut dyn DynChannelControl,
    limits: &DatagramLimits,
    sender: DirectUdpResponseSource,
    payload: &[u8],
) -> Result<(), EngineError> {
    let response = record_direct_udp_response_parts(runtime.services(), dispatch, sender, payload);
    write_direct_response(&response, || {
        send(
            tx,
            control,
            limits,
            &response.target,
            response.port,
            response.payload,
            dispatch.client_session_id(response.accounting.session_id()),
        )
    })
    .await?;
    Ok(())
}
pub(super) async fn upstream(
    runtime: &UdpIngressRuntime,
    dispatch: &mut UdpDispatch,
    tx: &mut Tx,
    control: &mut dyn DynChannelControl,
    limits: &DatagramLimits,
    incoming: crate::runtime::udp_flow::response::UpstreamUdpResponse,
) -> Result<(), EngineError> {
    let response = record_upstream_udp_response_received(
        runtime.services(),
        dispatch,
        runtime.services().udp_upstream_idle_timeout(),
        incoming,
    );
    write_upstream_response(&response, || {
        send(
            tx,
            control,
            limits,
            &response.target,
            response.port,
            &response.payload,
            dispatch.client_session_id(response.accounting.session_id()),
        )
    })
    .await?;
    Ok(())
}
pub(super) async fn chain(
    runtime: &UdpIngressRuntime,
    dispatch: &UdpDispatch,
    tx: &mut Tx,
    control: &mut dyn DynChannelControl,
    limits: &DatagramLimits,
    incoming: crate::runtime::udp_flow::response_join::ObservedChainJoin,
) -> Result<(), EngineError> {
    match incoming {
        Ok(Ok((target, port, payload, session))) => {
            let response = record_chain_udp_response_parts(
                runtime.services(),
                dispatch,
                target,
                port,
                payload,
                session,
            );
            write_chain_response(&response, || {
                send(
                    tx,
                    control,
                    limits,
                    &response.target,
                    response.port,
                    &response.payload,
                    dispatch.client_session_id(response.accounting.session_id()),
                )
            })
            .await?;
        }
        Ok(Err(error)) => tracing::warn!(error = %error, "chain upstream read error"),
        Err(error) => tracing::warn!(error = %error, "chain response task failed"),
    }
    Ok(())
}
