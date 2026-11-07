//! Same standalone root acquisition fences and actual retained flow observations.
use super::*;

impl WireguardAdapter {
    pub(crate) async fn fence_standalone_acquisition(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDeviceFenced>, EngineError> {
        let roots = {
            let current = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            current
                .values()
                .filter_map(|profile| profile.graph.as_ref())
                .map(|root| {
                    root.upgrade()
                        .ok_or_else(|| invalid("actual standalone factory owner was released"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut pauses = Vec::new();
        for root in roots {
            for peer in &root.peers {
                let pause = peer
                    .pause
                    .get()
                    .ok_or_else(|| invalid("actual standalone wire checkpoint is unavailable"))?;
                pauses.push(pause.fence(deadline).await.map_err(std::io::Error::other)?);
            }
        }
        Ok(pauses)
    }
    pub(crate) async fn pause_standalone_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDevicePaused>, EngineError> {
        let roots = {
            let current = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            current
                .values()
                .filter_map(|profile| profile.graph.as_ref())
                .map(|root| {
                    root.upgrade()
                        .ok_or_else(|| invalid("actual standalone factory owner was released"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut pauses = Vec::new();
        for root in roots {
            for peer in &root.peers {
                let pause = peer
                    .pause
                    .get()
                    .ok_or_else(|| invalid("actual standalone wire checkpoint is unavailable"))?;
                pauses.push(pause.pause(deadline).await.map_err(std::io::Error::other)?);
            }
        }
        Ok(pauses)
    }
}

impl StandaloneStackOptions {
    pub(in crate::adapters::wireguard::lifecycle) fn actual_flow_sources(
        &self,
    ) -> Result<
        Vec<(
            Arc<SharedRawIpDevice>,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        )>,
        EngineError,
    > {
        (0..self.peers.len())
            .map(|index| {
                let device = self.device(index).map_err(std::io::Error::other)?;
                let original = self.original(index).map_err(std::io::Error::other)?;
                Ok((
                    device,
                    zero_platform_tokio::controlled_carrier::CarrierOrigin::from_listener(original),
                ))
            })
            .collect()
    }
}
