//! Original Browser controller and protocol admission remain one installed owner.
use super::*;
use tokio::time::Instant;
pub struct Combined {
    browser: Owner,
    inner: Box<dyn DynResourceLifecycle>,
    browser_report: Option<CloseReport>,
    inner_report: Option<CloseReport>,
    failure: Option<Arc<ChannelError>>,
}
impl Combined {
    pub fn wrap(
        browser: Owner,
        inner: Box<dyn DynResourceLifecycle>,
    ) -> Box<dyn DynResourceLifecycle> {
        erase_lifecycle(Self {
            browser,
            inner,
            browser_report: None,
            inner_report: None,
            failure: None,
        })
    }
}
impl ResourceLifecycle for Combined {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.browser.start(end).await?;
        self.inner.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        self.browser.revoke(reason);
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut failed = false;
        if self
            .browser_report
            .as_ref()
            .is_none_or(|r| !r.is_complete())
        {
            match self.browser.shutdown(end).await {
                Ok(report) => self.browser_report = Some(report),
                Err(source) => {
                    failed = true;
                    self.failure.get_or_insert_with(|| {
                        source
                            .retained_source()
                            .cloned()
                            .unwrap_or_else(|| Arc::new(source))
                    });
                }
            }
        }
        if self.inner_report.as_ref().is_none_or(|r| !r.is_complete()) {
            match self.inner.shutdown(end).await {
                Ok(report) => self.inner_report = Some(report),
                Err(source) => {
                    failed = true;
                    self.failure.get_or_insert_with(|| {
                        source
                            .retained_source()
                            .cloned()
                            .unwrap_or_else(|| Arc::new(source))
                    });
                }
            }
        }
        if failed {
            return Err(ChannelError::retained(
                self.failure.as_ref().unwrap().clone(),
            ));
        }
        // Receipts live in this persistent owner across every later await.
        let mut report = self.browser_report.take().unwrap();
        report.append(self.inner_report.take().unwrap());
        Ok(report)
    }
}
