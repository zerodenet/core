//! Keep the actual Service loan/controller together across the BoxStream API.
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::controlled_carrier::loan::LoanIo;
pub(super) fn into_stream(loan: StreamLoan) -> BoxStream {
    Box::pin(ServiceIo {
        io: Some(loan.io),
        owner: loan.owner,
    })
}
struct ServiceIo {
    io: Option<LoanIo>,
    owner: LoanOwner,
}
impl Drop for ServiceIo {
    fn drop(&mut self) {
        // Return the unique actual view before nonwaiting control settlement.
        // Unfinished closure remains with the same persistent Transfer/Host.
        drop(self.io.take());
        self.owner.cancel(CloseReason::Shutdown);
        let _ = self.owner.try_close();
    }
}
impl AsyncRead for ServiceIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(
            self.io
                .as_mut()
                .expect("unique service view retained until Drop"),
        )
        .poll_read(cx, bytes)
    }
}
impl AsyncWrite for ServiceIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(
            self.io
                .as_mut()
                .expect("unique service view retained until Drop"),
        )
        .poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.io
                .as_mut()
                .expect("unique service view retained until Drop"),
        )
        .poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.io
                .as_mut()
                .expect("unique service view retained until Drop"),
        )
        .poll_shutdown(cx)
    }
}
