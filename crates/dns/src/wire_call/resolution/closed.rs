//! A caller returns only with both original Query/native/descendant receipts.
use super::*;
pub struct DnsResolutionClosed {
    call: std::sync::Weak<Resolution>,
    queries: [Arc<DnsQueryCallClosed>; 2],
}
impl std::fmt::Debug for DnsResolutionClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsResolutionClosed")
            .field("queries", &self.queries)
            .finish_non_exhaustive()
    }
}
impl DnsResolutionClosed {
    pub fn matches(&self, call: &DnsResolutionCall) -> bool {
        self.call.ptr_eq(&Arc::downgrade(&call.0))
            && self
                .queries
                .iter()
                .zip(&call.0.calls)
                .all(|(closed, original)| closed.matches(original))
    }
    pub fn queries(&self) -> &[Arc<DnsQueryCallClosed>; 2] {
        &self.queries
    }
}
/// Unknown keeps this exact caller and every original fault, not an error
/// string or a synthetic no-native/Scope-complete observation.
#[derive(Debug)]
pub struct DnsResolutionUnsettled {
    call: DnsResolutionCall,
    faults: [Option<Arc<io::Error>>; 2],
    conflict: bool,
}
impl DnsResolutionUnsettled {
    pub fn call(&self) -> &DnsResolutionCall {
        &self.call
    }
    pub fn faults(&self) -> impl Iterator<Item = &Arc<io::Error>> {
        self.faults.iter().flatten()
    }
}
impl std::fmt::Display for DnsResolutionUnsettled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.conflict {
            f.write_str("DNS resolution original coverage remains unknown")?;
        }
        for fault in self.faults() {
            write!(f, "; {fault}")?;
        }
        Ok(())
    }
}
impl std::error::Error for DnsResolutionUnsettled {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.faults().next().map(|e| e.as_ref() as _)
    }
}
impl DnsResolutionCall {
    pub async fn wait_closed(
        &self,
        end: tokio::time::Instant,
    ) -> io::Result<Arc<DnsResolutionClosed>> {
        if let Some(closed) = self.closed() {
            return Ok(closed);
        }
        // Both independent originals progress even if the first closes with fault.
        let (a, b) = tokio::join!(
            self.0.calls[0].wait_closed(end),
            self.0.calls[1].wait_closed(end)
        );
        // Logical/native receipt production is independent of Host acceptance.
        // Only this caller's final aggregate waits for each actual registration
        // to be released; it neither acknowledges nor drives another task.
        async fn accepted(
            result: io::Result<Arc<DnsQueryCallClosed>>,
            end: tokio::time::Instant,
        ) -> io::Result<Arc<DnsQueryCallClosed>> {
            let closed = result?;
            if let Some(query) = closed.query() {
                let joined = tokio::time::timeout_at(end, query.wait_registration_released())
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                if !query.matches_joined(&joined) || !joined.registration_released() {
                    return Err(io::Error::other(
                        "DNS resolution actual query registration remains unreleased",
                    ));
                }
            }
            Ok(closed)
        }
        let (a, b) = tokio::join!(accepted(a, end), accepted(b, end));
        let conflict = self.0.conflict.load(Ordering::Acquire);
        match (a, b) {
            (Ok(a), Ok(b)) if !conflict => Ok(self
                .0
                .closed
                .get_or_init(|| {
                    Arc::new(DnsResolutionClosed {
                        call: Arc::downgrade(&self.0),
                        queries: [a, b],
                    })
                })
                .clone()),
            (a, b) => Err(io::Error::other(DnsResolutionUnsettled {
                call: self.clone(),
                faults: [a.err().map(Arc::new), b.err().map(Arc::new)],
                conflict,
            })),
        }
    }
}
