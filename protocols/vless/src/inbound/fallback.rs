use super::VlessFallbackReplay;
use crate::fallback::FallbackPolicy;
use zero_traits::{AsyncSocket, FallbackRoute};

impl<S: AsyncSocket> VlessFallbackReplay<S> {
    pub(crate) async fn select_route(
        &mut self,
        policy: &FallbackPolicy,
        name: Option<&str>,
        alpn: Option<&str>,
        source: Option<core::net::SocketAddr>,
        destination: Option<core::net::SocketAddr>,
    ) -> Result<(), zero_core::Error> {
        if policy.is_empty() {
            return Ok(());
        }
        // The request parser may have rejected just the first byte. Retain a
        // bounded HTTP prefix for path selection and replay every consumed byte.
        if self.replay_head.first().is_some_and(u8::is_ascii_uppercase) {
            while self.replay_head.len() < 64 {
                let head = &self.replay_head;
                if head.len() >= 18
                    && (head.contains(&b'\n') || head.iter().filter(|b| **b == b' ').count() >= 2)
                {
                    break;
                }
                let mut bytes = [0; 64];
                let remaining = 64 - head.len();
                let read = self
                    .stream
                    .read(&mut bytes[..remaining])
                    .await
                    .map_err(|_| zero_core::Error::Io("fallback prefix read failed"))?;
                if read == 0 {
                    break;
                }
                self.replay_head.extend_from_slice(&bytes[..read]);
            }
        }
        let rule = policy
            .select(name.unwrap_or(""), alpn.unwrap_or(""), &self.replay_head)
            .ok_or(zero_core::Error::Unsupported(
                "no matching VLESS fallback rule",
            ))?;
        self.selected = Some(FallbackRoute {
            endpoint: rule.endpoint.clone(),
            proxy_protocol: rule.proxy_protocol,
            source,
            destination,
        });
        Ok(())
    }
}
