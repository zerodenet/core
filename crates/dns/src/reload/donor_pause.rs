//! The existing actual reload token holds its query pause through commit/discard.
use super::*;
impl DnsSystem {
    pub fn prepare_donor_pause(
        &self,
        resources: &DnsReloadResources,
        replaced: &[zero_core::component::ComponentInstanceId],
    ) -> io::Result<()> {
        {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other(
                    "DNS donor pause requires its actual current reload token",
                ));
            }
            let pause = resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned");
            if let Some(pause) = pause.as_ref() {
                return if pause.matches_replacement(replaced) {
                    Ok(())
                } else {
                    Err(io::Error::other(
                        "actual DNS token already names different replaced originals",
                    ))
                };
            }
        }
        // Preparing may inspect/join-observe an original query and release
        // actual witnesses. No token/cache/state lock spans these callbacks.
        let pause = Arc::new(self.try_pause_donor_replacement(replaced)?);
        {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other("DNS donor pause token was discarded"));
            }
            let mut installed = resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned");
            if let Some(current) = installed.as_ref() {
                if !current.matches_replacement(replaced) {
                    return Err(io::Error::other(
                        "DNS donor pause token changed its actual replacement set",
                    ));
                }
            } else {
                *installed = Some(pause.clone());
            }
        }
        // A concurrent duplicate preparation restores only its own pause count.
        drop(pause);
        Ok(())
    }
    pub async fn retire_paused_donors(
        &self,
        resources: &DnsReloadResources,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        let pause = {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other(
                    "DNS donor retirement requires its actual current reload token",
                ));
            }
            resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned")
                .clone()
                .ok_or_else(|| io::Error::other("actual DNS donor pause was not installed"))?
        };
        // Cancellation retains this same guard in prepared_reload. No mutex,
        // Scope join-consumer, or fresh resource owner crosses this await.
        pause.retire(end).await
    }
}
