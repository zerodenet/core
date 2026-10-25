//! Retry authority comes from the original controller, never an error kind.
use super::*;
use zero_platform_tokio::controlled_carrier::{loan::LoanClosed, CarrierOriginClosed};

pub(crate) struct Attempt {
    result: io::Result<Vec<u8>>,
    closed: Option<Proof>,
}
enum Proof {
    Carrier(LoanClosed, CarrierOriginClosed),
    #[cfg(feature = "doq")]
    Metadata(
        zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolClosed,
    ),
}
impl Attempt {
    pub(crate) fn blocked(source: io::Error) -> Self {
        Self {
            result: Err(source),
            closed: None,
        }
    }
    pub(super) fn can_retry(&self) -> bool {
        self.closed.as_ref().is_some_and(|proof| match proof {
            Proof::Carrier(logical, native) => {
                native.matches(logical.original()) && native.had_native_acquisition()
            }
            #[cfg(feature = "doq")]
            Proof::Metadata(original, native) => {
                native.matches(original) && native.had_native_acquisition()
            }
        })
    }
    pub(super) fn failed(&self) -> bool {
        self.result.is_err()
    }
    pub(super) fn is_truncated(&self, query: &[u8]) -> bool {
        self.can_retry()
            && self.result.as_ref().is_ok_and(|response| {
                crate::message::parse_response(query, response).is_ok_and(|parsed| parsed.truncated)
            })
    }
    pub(crate) fn into_parts(self) -> (io::Result<Vec<u8>>, bool) {
        let settled = self.can_retry();
        (self.result, settled)
    }
}
pub(super) async fn settle(
    owner: &mut LoanOwner,
    end: Instant,
    result: io::Result<Vec<u8>>,
) -> Attempt {
    let confirmation = async {
        let report = owner.close(end).await.map_err(io::Error::other)?;
        let logical = owner.closed().filter(|closed| closed.matches(owner));
        let native = owner
            .original()
            .closed()
            .filter(|closed| closed.matches(owner.original()) && closed.had_native_acquisition());
        match (report.is_complete(), logical, native) {
            (true, Some(logical), Some(native)) => Ok((logical, native)),
            _ => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "DNS original close remains unconfirmed",
            )),
        }
    }
    .await;
    match confirmation {
        Ok(closed) => Attempt {
            result,
            closed: Some(Proof::Carrier(closed.0, closed.1)),
        },
        Err(close) => Attempt::blocked(io::Error::new(
            close.kind(),
            CloseFailure {
                query: result.err(),
                close,
            },
        )),
    }
}
#[cfg(feature = "doq")]
pub(super) async fn settle_quic(
    owner: &mut dyn crate::native::NativeDnsQuicControl,
    end: Instant,
    result: io::Result<Vec<u8>>,
) -> Attempt {
    let original = owner.original().clone();
    match owner.close(end).await {
        Ok(closed) if closed.matches(&original) && closed.had_native_acquisition() => Attempt {
            result,
            closed: Some(Proof::Metadata(original, closed)),
        },
        closed => {
            let close = closed.err().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DoQ original close remains unconfirmed",
                )
            });
            Attempt::blocked(io::Error::new(
                close.kind(),
                CloseFailure {
                    query: result.err(),
                    close,
                },
            ))
        }
    }
}
#[derive(Debug)]
struct CloseFailure {
    query: Option<io::Error>,
    close: io::Error,
}
impl std::fmt::Display for CloseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(query) = &self.query {
            write!(formatter, "query failed: {query}; ")?;
        }
        write!(formatter, "original DNS close failed: {}", self.close)
    }
}
impl std::error::Error for CloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.close)
    }
}
