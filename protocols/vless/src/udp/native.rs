//! Keep existing XUDP framing/global identity on the actual shared native pool.
use super::*;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::{pooled_driver::datagram::PacketIo, RuntimeError};
impl PreparedVlessUdpFlowPlan {
    pub(crate) fn uses_xudp(&self) -> bool {
        self.plan.resume().mux_flow_enabled()
    }
    pub(crate) async fn open_native_mux_flow<S, F, Fut>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        pool: &crate::mux_pool::MuxConnectionPool,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<VlessUdpFlowConnection, RuntimeError>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        let resume = self.plan.resume();
        resume.ensure_udp_supported(session.port)?;
        let key = resume
            .udp_mux_pool_key_from_transport_config(
                server,
                port,
                self.mux_transport_profile.as_borrowed(),
            )
            .ok_or(Error::Config("native XUDP requires an enabled plan"))?;
        let concurrency = resume
            .xudp_concurrency()
            .ok_or(Error::Config("native XUDP concurrency missing"))?;
        let opened = pool
            .open_native_udp_with_id(
                key,
                concurrency,
                binding,
                None,
                Some(session.id.to_be_bytes()),
                wrap,
            )
            .await?;
        let (send_tx, mut send_rx) = mpsc::channel::<VlessUdpFlowSend>(32);
        let (responses, _) = broadcast::channel::<VlessUdpFlowResponse>(32);
        let response_tx = responses.clone();
        let association = zero_core::component::AssociationId(session.id);
        // This replaces the existing logical compatibility pump, not the pool
        // driver. Custody remains installed until its real packet view drops.
        tokio::spawn(async move {
            let mut opened = opened;
            loop {
                tokio::select! {
                    request=send_rx.recv()=>{
                        let Some(request)=request else {break;};
                        let (address,port,payload)=request.packet.into_parts();let size=payload.len();
                        let packet=zero_core::packet::Datagram{association,address:zero_core::request::Destination::new(address,port),payload:payload.into()};
                        let result=opened.io.send(&packet).map(|_|size).map_err(|_|Error::Io("vless native XUDP send"));
                        let failed=result.is_err();let _=request.result_tx.send(result);if failed {break;}
                    }
                    packet=std::future::poll_fn(|cx|opened.io.poll_recv(cx))=>match packet {
                        Ok(Some((target,payload)))=>{let _=response_tx.send((target.address,target.port,payload));}
                        _=>break,
                    }
                }
            }
            // PooledDatagram drops its actual packet I/O before the PoolUse.
            drop(opened);
        });
        Ok(VlessUdpFlowConnection::new(VlessUdpFlowHandle {
            sender: VlessUdpFlowSender {
                send_tx,
                block_udp443: false,
            },
            responses,
        }))
    }
}
