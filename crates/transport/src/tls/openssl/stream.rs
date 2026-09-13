use std::{
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use foreign_types::ForeignTypeRef;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::TransportBypassControl;

use super::ffi;
use crate::tls::record_boundary::TlsRecordBoundary;

pub(crate) struct OpenSslTlsStream<S> {
    pub(super) stream: tokio_openssl::SslStream<TlsRecordBoundary<S>>,
    pub(super) control: Option<TransportBypassControl>,
    pub(super) failed: bool,
}

impl<S> OpenSslTlsStream<S> {
    pub(crate) async fn accept(context: &super::OpenSslServerContext, socket: S) -> io::Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ssl = context.new_server_ssl()?;
        let control = TransportBypassControl::default();
        let mut stream = tokio_openssl::SslStream::new(
            ssl,
            TlsRecordBoundary::with_control(socket, control.clone()),
        )
        .map_err(io::Error::other)?;
        Pin::new(&mut stream)
            .accept()
            .await
            .map_err(io::Error::other)?;
        let control = is_tls13(&stream).then_some(control);
        Ok(Self {
            stream,
            control,
            failed: false,
        })
    }

    pub(crate) fn control(&self) -> Option<TransportBypassControl> {
        self.control.clone()
    }

    pub(crate) fn alpn_protocol(&self) -> Option<&[u8]> {
        self.stream.ssl().selected_alpn_protocol()
    }

    pub(crate) fn server_name(&self) -> Option<&str> {
        self.stream
            .ssl()
            .servername(openssl::ssl::NameType::HOST_NAME)
    }

    #[cfg(test)]
    pub(super) fn session_reused(&self) -> bool {
        self.stream.ssl().session_reused()
    }

    #[cfg(test)]
    pub(super) fn ech_status(&self) -> (i32, Option<String>, Option<String>) {
        let mut inner_name = std::ptr::null_mut();
        let mut outer_name = std::ptr::null_mut();
        unsafe {
            let status = ffi::SSL_ech_get1_status(
                self.stream.ssl().as_ptr(),
                &mut inner_name,
                &mut outer_name,
            );
            let inner_name = take_openssl_name(inner_name);
            let outer_name = take_openssl_name(outer_name);
            (status, inner_name, outer_name)
        }
    }

    fn raw_read(&self) -> bool {
        self.control
            .as_ref()
            .is_some_and(TransportBypassControl::read_bypass_requested)
    }

    fn raw_write(&self) -> bool {
        self.control
            .as_ref()
            .is_some_and(TransportBypassControl::write_bypass_requested)
    }

    fn ensure_raw_read_safe(&self) -> io::Result<()> {
        if self.stream.ssl().pending() != 0
            || unsafe { ffi::SSL_has_pending(self.stream.ssl().as_ptr()) } != 0
        {
            return Err(io::Error::other(
                "OpenSSL retained TLS input at raw read handoff",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
unsafe fn take_openssl_name(name: *mut core::ffi::c_char) -> Option<String> {
    if name.is_null() {
        return None;
    }
    let value = unsafe { std::ffi::CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned();
    unsafe { openssl_sys::OPENSSL_free(name.cast()) };
    Some(value)
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for OpenSslTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::Error::other("OpenSSL TLS stream failed")));
        }
        if this.raw_read() {
            if let Err(error) = this.ensure_raw_read_safe() {
                this.failed = true;
                return Poll::Ready(Err(error));
            }
            return Pin::new(this.stream.get_mut()).raw_poll_read(cx, output);
        }
        let result = ready!(Pin::new(&mut this.stream).poll_read(cx, output));
        if result.is_err() {
            this.failed = true;
        }
        Poll::Ready(result)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for OpenSslTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if this.raw_write() {
            return Pin::new(this.stream.get_mut()).raw_poll_write(cx, bytes);
        }
        let result = ready!(Pin::new(&mut this.stream).poll_write(cx, bytes));
        if result.is_err() {
            this.failed = true;
        }
        Poll::Ready(result)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.raw_write() {
            Pin::new(this.stream.get_mut()).raw_poll_flush(cx)
        } else {
            Pin::new(&mut this.stream).poll_flush(cx)
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.raw_read() || this.raw_write() {
            Pin::new(this.stream.get_mut()).raw_poll_shutdown(cx)
        } else {
            Pin::new(&mut this.stream).poll_shutdown(cx)
        }
    }
}

pub(super) fn is_tls13<S>(stream: &tokio_openssl::SslStream<S>) -> bool {
    stream.ssl().version_str() == "TLSv1.3"
}
