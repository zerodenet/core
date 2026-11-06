//! Existing fixed-target raw UDP codec over the same supervised physical pool.
use super::*;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::RuntimeError;
impl PreparedTrojanUdpFlowPlan {
    pub(crate) async fn open_native_mux_flow<S, F, Fut>(
        &self,
        session: &Session,
        pool: &crate::mux::TrojanMuxConnectionPool,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<TrojanUdpFlowConnection, RuntimeError>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        let mux = self
            .mux
            .as_ref()
            .ok_or(Error::Config("native UDP requires MUX"))?;
        let key = crate::mux::pool_key_from_config(
            &mux.server,
            mux.port,
            &mux.password,
            mux.sni.as_deref(),
            mux.insecure,
            mux.client_fingerprint.as_deref(),
            mux.idle_timeout_secs,
            mux.response_backlog,
        );
        let io = pool
            .open_native_udp_stream(
                key,
                mux.concurrency,
                session.target.clone(),
                session.port,
                binding,
                wrap,
            )
            .await?;
        Ok(start_native_mux_flow(
            io,
            session.target.clone(),
            session.port,
        ))
    }
    pub(crate) fn native_tls_profile(&self) -> OwnedTrojanResolvedTlsProfile {
        self.plan.owned_tls_profile(None)
    }
}

// Keep one logical compatibility pump. A retained sender is no longer allowed
// to pin the writer/PoolUse after the original physical reader has closed.
fn start_native_mux_flow<S>(stream: S, target: Address, port: u16) -> TrojanUdpFlowConnection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (send_tx, mut send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel::<UdpFlowPacket>(32);
    let returned = responses.clone();
    let fixed_target = target.clone();
    let send_target = target.clone();
    tokio::spawn(async move {
        let send = async {
            while let Some(packet) = send_rx.recv().await {
                if packet.target != send_target
                    || packet.port != port
                    || packet.payload.len() > crate::mux::MUX_MAX_DATA_LEN
                {
                    break;
                }
                if writer.write_all(&packet.payload).await.is_err() || writer.flush().await.is_err()
                {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        };
        let receive = async {
            let mut bytes = vec![0; crate::mux::MUX_MAX_DATA_LEN];
            while let Ok(size) = reader.read(&mut bytes).await {
                if size == 0 {
                    break;
                }
                if returned
                    .send(UdpFlowPacket::new(
                        target.clone(),
                        port,
                        bytes[..size].to_vec(),
                    ))
                    .is_err()
                {
                    break;
                }
            }
        };
        tokio::select! {_ = send=>{},_ = receive=>{}}
        // Both real split views end together; logical return is still not a
        // substitute for the parent's original native/task close receipts.
    });
    TrojanUdpFlowConnection::new(TrojanUdpFlowSession::new(TrojanUdpFlowHandle {
        sender: TrojanUdpFlowSender {
            send_tx,
            fixed_target: Some((fixed_target, port)),
            max_payload_len: Some(crate::mux::MUX_MAX_DATA_LEN),
        },
        responses,
    }))
}
