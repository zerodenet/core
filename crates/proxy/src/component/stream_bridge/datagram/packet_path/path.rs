//! Same installed standard Datagram session, views and logical control.
use super::*;
pub(super) struct Path {
    tx: tokio::sync::Mutex<Option<Pin<Box<dyn DatagramTx>>>>,
    rx: tokio::sync::Mutex<Option<Pin<Box<dyn DatagramRx>>>>,
    control: tokio::sync::Mutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>,
    limits: DatagramLimits,
    association: AssociationId,
    stop: CancellationToken,
    handoff: Option<PacketDriverDatagramHandoff>,
}
impl Path {
    pub(super) fn new(
        session: BoxDatagramSession,
        association: AssociationId,
        stop: CancellationToken,
        handoff: Option<PacketDriverDatagramHandoff>,
    ) -> Self {
        Self {
            tx: tokio::sync::Mutex::new(Some(session.tx)),
            rx: tokio::sync::Mutex::new(Some(session.rx)),
            control: tokio::sync::Mutex::new(session.control),
            limits: session.limits,
            association,
            stop,
            handoff,
        }
    }
}
#[async_trait::async_trait]
impl PacketPathCarrier for Path {
    fn has_driver_handoff(&self) -> bool {
        self.handoff.is_some()
    }
    fn prepare_codec_send(
        &self,
        lengths: &[usize],
    ) -> Result<zero_platform_tokio::contract::component::PacketDriverSendBuffers, ChannelError>
    {
        self.handoff
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "independent library path has no Source memory",
                )
            })?
            .prepare_send(lengths)
    }
    async fn send_codec_buffered(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [zero_traits::PacketBuffer],
        copy: &mut zero_platform_tokio::contract::component::PacketDriverSendCopy,
    ) -> Result<(), EngineError> {
        if !buffers.is_empty() {
            return Err(failure(invalid(
                Reason::InvalidConfig,
                "unused codec workspace layout",
            )));
        }
        let frame = copy
            .copy_datagram(self.association, target, port, payload)
            .map_err(failure)?;
        let mut tx = self.tx.lock().await;
        let tx = tx
            .as_mut()
            .ok_or_else(|| failure(invalid(Reason::Revoked, "same packet sender was returned")))?;
        let mut control = self.control.lock().await;
        zero_platform_tokio::contract::datagram::send(
            tx.as_mut(),
            control.as_mut(),
            &frame,
            &self.limits,
        )
        .await
        .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        Ok(())
    }

    fn request_driver_retirement(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        let handoff = self.handoff.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "same path has no Packet driver handoff",
            )
        })?;
        if !handoff.allows_retirement(driver) {
            return Err(invalid(
                Reason::StalePlan,
                "Packet path belongs to another receiver",
            ));
        }
        handoff.request_retirement();
        Ok(())
    }

    fn confirm_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        self.handoff
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "independent library packet path has no original wire handoff",
                )
            })?
            .confirm_receiver(driver)
    }

    async fn retire_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        let handoff = self
            .handoff
            .as_ref()
            .filter(|handoff| handoff.matches_receiver(driver))
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "carrier retirement does not name its actual installed wire",
                )
            })?;
        // Cancel only this attempt. The original wire/reconnect token stays
        // live to drive retirement and its original retry/backoff future.
        handoff.request_retirement();
        let views = tokio::time::timeout_at(deadline, async {
            let (mut tx, mut rx) = tokio::join!(self.tx.lock(), self.rx.lock());
            (tx.take(), rx.take())
        })
        .await
        .map_err(|source| {
            invalid(Reason::Timeout, "same pending packet views did not return").with_source(source)
        })?;
        drop(views);
        // The reservation wrapper must observe its same controller completion
        // too; bypassing it would strand the original association credit.
        let mut control = tokio::time::timeout_at(deadline, self.control.lock())
            .await
            .map_err(|source| {
                invalid(Reason::Timeout, "same packet control remains borrowed").with_source(source)
            })?;
        let mut report = control.wait_closed(deadline).await?;
        drop(control);
        report.append(handoff.wait_closed(deadline).await?);
        Ok(report)
    }

    async fn send_to(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        if self.handoff.is_some() {
            let prepared = self.prepare_codec_send(&[]).map_err(failure)?;
            let (mut buffers, mut copy) = prepared.into_parts();
            return self
                .send_codec_buffered(target, port, payload, &mut buffers, &mut copy)
                .await;
        }
        // Explicit independent library caller, never a missing Kernel grant.
        let frame = zero_core::packet::Datagram {
            association: self.association,
            address: Destination::new(target.clone(), port),
            payload: payload.to_vec().into(),
        };
        let mut tx = self.tx.lock().await;
        let mut control = self.control.lock().await;
        let tx = tx.as_mut().ok_or_else(|| {
            failure(invalid(
                Reason::Revoked,
                "same packet-path sender was returned",
            ))
        })?;
        zero_platform_tokio::contract::datagram::send(
            tx.as_mut(),
            control.as_mut(),
            &frame,
            &self.limits,
        )
        .await
        .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        Ok(())
    }
    async fn recv_from(&self, bytes: &mut [u8]) -> Result<usize, EngineError> {
        let mut rx = self.rx.lock().await;
        let rx = rx.as_mut().ok_or_else(|| {
            failure(invalid(
                Reason::Revoked,
                "same packet-path receiver was returned",
            ))
        })?;
        let frame = zero_platform_tokio::contract::datagram::recv(rx.as_mut())
            .await
            .map_err(failure)?
            .ok_or_else(|| {
                failure(invalid(
                    Reason::Revoked,
                    "standard packet-path receiver closed",
                ))
            })?;
        if frame.payload.len() > bytes.len() {
            return Err(failure(invalid(
                Reason::Capacity,
                "standard packet-path receive buffer too small",
            )));
        }
        bytes[..frame.payload.len()].copy_from_slice(&frame.payload);
        Ok(frame.payload.len())
    }
}
impl Drop for Path {
    fn drop(&mut self) {
        if let Some(handoff) = &self.handoff {
            handoff.request_retirement();
        } else {
            self.stop.cancel();
        }
        if let Ok(control) = self.control.try_lock() {
            control.cancel(CloseReason::Shutdown);
        }
    }
}
