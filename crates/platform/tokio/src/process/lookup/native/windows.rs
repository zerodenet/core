//! The matching metadata HANDLE. Unknown close is durable and never retried.
use super::*;
enum Disposition {
    Unattempted,
    Attempting,
    Open {
        raw: usize,
        failure: Option<Arc<ChannelError>>,
    },
    Closed,
}
pub(in crate::process) struct Native {
    state: Mutex<Disposition>,
}
impl Native {
    pub(in crate::process) fn new() -> Self {
        Self {
            state: Mutex::new(Disposition::Unattempted),
        }
    }
    pub(in crate::process) fn confirmed(&self) -> bool {
        matches!(
            *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Disposition::Unattempted | Disposition::Closed
        )
    }
    pub(in crate::process) fn failure(&self) -> Option<Arc<ChannelError>> {
        match &*self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            Disposition::Open { failure, .. } => failure.clone(),
            _ => None,
        }
    }
    pub(in crate::process) fn collect_after_join(&self) -> Result<(), Arc<ChannelError>> {
        let raw = match &*self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            Disposition::Open { raw, failure: None } => Some(*raw),
            Disposition::Open {
                failure: Some(source),
                ..
            } => return Err(source.clone()),
            Disposition::Attempting => {
                return Err(Arc::new(ChannelError::new(
                    Stage::Close,
                    Reason::Io,
                    Impact::Component,
                )))
            }
            _ => None,
        };
        if let Some(raw) = raw {
            self.close(raw)?;
        }
        Ok(())
    }
    fn close(&self, matching: usize) -> Result<(), Arc<ChannelError>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Disposition::Open { raw, failure } = &mut *state else {
            return Err(Arc::new(error(Stage::Close, Reason::StalePlan)));
        };
        if *raw != matching {
            return Err(Arc::new(error(Stage::Close, Reason::StalePlan)));
        }
        if let Some(source) = failure {
            return Err(source.clone());
        }
        let closed =
            unsafe { windows_sys::Win32::Foundation::CloseHandle(*raw as *mut std::ffi::c_void) };
        if closed == 0 {
            let source = Arc::new(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(std::io::Error::last_os_error()),
            );
            *failure = Some(source.clone());
            return Err(source);
        }
        *state = Disposition::Closed;
        Ok(())
    }
}
impl LocalProcessLookup {
    pub(in crate::process) fn begin_native(&self) -> Result<(), Arc<ChannelError>> {
        let mut state = self
            .native
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, Disposition::Unattempted) {
            return Err(Arc::new(error(Stage::Prepare, Reason::StalePlan)));
        }
        *state = Disposition::Attempting;
        Ok(())
    }
    pub(in crate::process) fn opened_native(&self, raw: usize) {
        let mut state = self
            .native
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expected = matches!(*state, Disposition::Attempting);
        *state = if raw == 0 {
            Disposition::Closed
        } else {
            Disposition::Open { raw, failure: None }
        };
        debug_assert!(expected, "same metadata acquisition attempt");
    }
    pub(in crate::process) fn close_native(
        &self,
        matching: usize,
    ) -> Result<(), Arc<ChannelError>> {
        self.native.close(matching)
    }
}
