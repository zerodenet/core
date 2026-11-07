use super::*;
impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Clone one existing metadata row at a time. No owner mutex is held
        // while formatting foreign error sources or invoking the formatter.
        f.write_str("PacketDriverCleanupFailures[")?;
        let count = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        for index in 0..count {
            let row = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .cloned();
            let Some(row) = row else { continue };
            if let Some(source) = row.final_control.0.get() {
                write!(f, "final={source:?};")?;
            }
            let count = row
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            for index in 0..count {
                let phase = row
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(index)
                    .cloned();
                let Some(phase) = phase else { continue };
                if let Some(source) = phase.native.0.get() {
                    write!(f, "native={source:?};")?;
                }
                if let Some(source) = phase.logical.0.get() {
                    write!(f, "logical={source:?};")?;
                }
            }
            let count = row
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            for index in 0..count {
                let phase = row
                    .streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(index)
                    .cloned();
                if let Some(source) = phase.as_ref().and_then(|same| same.0.get()) {
                    write!(f, "prefix={source:?};")?;
                }
            }
        }
        let count = self
            .actors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        for index in 0..count {
            let row = self
                .actors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .cloned();
            let Some(row) = row else { continue };
            if let Some(source) = row.result.0.get() {
                write!(f, "actor={source:?};")?;
            }
            if let Some(source) = row.acceptance.0.get() {
                write!(f, "acceptance={source:?};")?;
            }
        }
        f.write_str("]")
    }
}
