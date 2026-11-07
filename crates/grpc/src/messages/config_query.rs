//! Synchronous borrowed Engine encoding, with no guard across a quota await.
use super::{output::Lease, Record};
use std::{io, sync::Arc};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};

enum Credit {
    Wire(Arc<Lease>),
    Status(ControlBudgetPermit),
}
enum Prepared {
    Json { bytes: Vec<u8>, frame: usize },
    Status(String),
}
enum Failure {
    Busy,
    Status(tonic::Status),
}
fn capacity(error: io::Error) -> Failure {
    if error.kind() == io::ErrorKind::WouldBlock {
        Failure::Busy
    } else {
        Failure::Status(tonic::Status::resource_exhausted(error.to_string()))
    }
}
fn overflow() -> Failure {
    Failure::Status(tonic::Status::resource_exhausted(
        "gRPC response exceeds its admitted message limit",
    ))
}
struct Count {
    bytes: usize,
    limit: usize,
}
impl io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("gRPC response exceeds its admitted message limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl std::fmt::Write for Count {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        io::Write::write_all(self, value.as_bytes()).map_err(|_| std::fmt::Error)
    }
}
impl Record {
    /// Called after the concrete producer returned its error and its guards.
    /// Measure the same borrowed Display before constructing Status's String.
    pub(crate) async fn error_status(
        &self,
        code: tonic::Code,
        prefix: &str,
        error: &(impl std::fmt::Display + Sync),
    ) -> tonic::Status {
        let mut count = Count {
            bytes: 0,
            limit: self.policy.response_bytes,
        };
        if std::fmt::write(&mut count, format_args!("{prefix}{error}")).is_err() {
            return tonic::Status::resource_exhausted(
                "gRPC error exceeds its admitted message limit",
            );
        }
        let Some(bound) = count
            .bytes
            .checked_mul(7)
            .and_then(|size| size.checked_add(super::CODEC_BUFFER_SIZE))
        else {
            return tonic::Status::resource_exhausted("gRPC error storage length overflow");
        };
        let credit = loop {
            let changed = self.budget.capacity_changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.budget.reserve(ControlCharge::ResponseBytes(bound)) {
                Ok(credit) => break credit,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => changed.await,
                Err(error) => return tonic::Status::resource_exhausted(error.to_string()),
            }
        };
        let mut message = String::new();
        if let Err(error) = message.try_reserve_exact(count.bytes) {
            return tonic::Status::resource_exhausted(error.to_string());
        }
        use std::fmt::Write;
        if write!(&mut message, "{prefix}{error}").is_err() || message.len() != count.bytes {
            return tonic::Status::internal("same gRPC error changed during encoding");
        }
        let previous = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(credit);
        drop(previous);
        tonic::Status::new(code, message)
    }
    pub(crate) async fn config_query(
        &self,
        engine: &zero_engine::EngineHandle,
        request: &zero_api::QueryRequest,
    ) -> Option<Result<Vec<u8>, tonic::Status>> {
        loop {
            let changed = self.budget.capacity_changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Both success and exceptional unwinding drop every borrowed
            // Engine guard before this actual credit can refund/wake callers.
            let mut credit = None;
            let prepared = prepare(self, engine, request, &mut credit)?;
            match prepared {
                Err(Failure::Busy) => {
                    drop(credit);
                    changed.await;
                }
                Err(Failure::Status(status)) => return Some(Err(status)),
                Ok(Prepared::Json { bytes, frame }) => {
                    let Some(Credit::Wire(credit)) = credit.take() else {
                        unreachable!("actual encoded owner")
                    };
                    self.enqueue(frame, credit);
                    return Some(Ok(bytes));
                }
                Ok(Prepared::Status(message)) => {
                    let Some(Credit::Status(credit)) = credit.take() else {
                        unreachable!("actual status owner")
                    };
                    let previous = self
                        .status
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .replace(credit);
                    drop(previous);
                    return Some(Err(tonic::Status::internal(message)));
                }
            }
        }
    }
    pub(crate) fn retain_status_header(&self, headers: &mut hyper::HeaderMap) {
        let Some(value) = headers.get_mut("grpc-message") else {
            return;
        };
        let credit = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(credit) = credit else { return };
        let original = std::mem::replace(value, hyper::header::HeaderValue::from_static(""));
        *value = hyper::header::HeaderValue::from_maybe_shared(hyper::body::Bytes::from_owner(
            StatusHeader {
                original,
                _credit: credit,
                _lifetime: self.lifetime.clone(),
            },
        ))
        .expect("same already validated original header bytes");
    }
}
struct StatusHeader {
    original: hyper::header::HeaderValue,
    _credit: ControlBudgetPermit,
    _lifetime: Arc<super::registry::Lifetime>,
}
impl AsRef<[u8]> for StatusHeader {
    fn as_ref(&self) -> &[u8] {
        self.original.as_bytes()
    }
}
fn prepare(
    original: &Record,
    engine: &zero_engine::EngineHandle,
    request: &zero_api::QueryRequest,
    credit: &mut Option<Credit>,
) -> Option<Result<Prepared, Failure>> {
    let snapshot = engine.inner().runtime_snapshot();
    let selected = snapshot.control_config_query(request)?;
    Some((|| match selected {
        Ok(view) => {
            let mut count = Count {
                bytes: 0,
                limit: original.policy.response_bytes,
            };
            serde_json::to_writer(&mut count, &view).map_err(|_| overflow())?;
            let mut length = count.bytes;
            let mut prefix = 1usize;
            while length >= 128 {
                length >>= 7;
                prefix += 1;
            }
            let frame = count.bytes.checked_add(prefix + 6).ok_or_else(overflow)?;
            *credit = Some(Credit::Wire(
                original
                    .encoded(frame, ControlCharge::ResponseBytes(count.bytes))
                    .map_err(capacity)?,
            ));
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(count.bytes).map_err(|error| {
                Failure::Status(tonic::Status::resource_exhausted(error.to_string()))
            })?;
            serde_json::to_writer(&mut bytes, &view)
                .map_err(|error| Failure::Status(tonic::Status::internal(error.to_string())))?;
            if bytes.len() != count.bytes {
                return Err(Failure::Status(tonic::Status::internal(
                    "same borrowed Engine response changed during encoding",
                )));
            }
            Ok(Prepared::Json { bytes, frame })
        }
        Err(missing) => {
            let size = missing
                .message_len()
                .and_then(|size| size.checked_add("not_found: ".len()))
                .filter(|size| *size <= original.policy.response_bytes)
                .ok_or_else(overflow)?;
            // Original Status String plus pinned Tonic's percent-encoded
            // header workspace/storage, prepaid before either is allocated.
            let bound = size
                .checked_mul(7)
                .and_then(|size| size.checked_add(super::CODEC_BUFFER_SIZE))
                .ok_or_else(overflow)?;
            *credit = Some(Credit::Status(
                original
                    .budget
                    .reserve(ControlCharge::ResponseBytes(bound))
                    .map_err(capacity)?,
            ));
            let mut message = String::new();
            message.try_reserve_exact(size).map_err(|error| {
                Failure::Status(tonic::Status::resource_exhausted(error.to_string()))
            })?;
            use std::fmt::Write;
            write!(&mut message, "not_found: {}", missing.message())
                .expect("String write is infallible");
            Ok(Prepared::Status(message))
        }
    })())
}

#[cfg(test)]
mod tests;
