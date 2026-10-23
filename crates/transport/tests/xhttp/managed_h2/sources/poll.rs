use super::*;
use std::task::Context;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

struct Fault(Option<io::Error>);
impl AsyncRead for Fault {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
}
impl AsyncWrite for Fault {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
}
fn native_error() -> io::Error {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err()
}

#[tokio::test]
async fn every_original_poll_error_retains_the_same_native_object_and_vectored_behavior() {
    for operation in [
        IoOperation::Read,
        IoOperation::Write,
        IoOperation::WriteVectored,
        IoOperation::Flush,
        IoOperation::Shutdown,
    ] {
        let native = native_error();
        let code = native.raw_os_error().expect("real native bind collision");
        let kind = native.kind();
        let failure = Arc::new(Failure::new(instance()));
        let mut io = ObservedIo::new(Fault(Some(native)), failure.clone());
        assert!(io.is_write_vectored());
        let facade = match operation {
            IoOperation::Read => io.read(&mut [0]).await.unwrap_err(),
            IoOperation::Write => io.write(b"x").await.unwrap_err(),
            IoOperation::WriteVectored => io
                .write_vectored(&[io::IoSlice::new(b"x")])
                .await
                .unwrap_err(),
            IoOperation::Flush => io.flush().await.unwrap_err(),
            IoOperation::Shutdown => io.shutdown().await.unwrap_err(),
        };
        let evidence = failure.evidence();
        let original = evidence.io_source().unwrap();
        assert_eq!(evidence.io_operation(), Some(operation));
        assert_eq!(facade.kind(), kind);
        assert_eq!(
            facade.raw_os_error(),
            None,
            "the facade is not the original native representation"
        );
        assert_eq!(original.raw_os_error(), Some(code));
        assert!(original.get_ref().is_none());
        let retained = facade
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert!(std::ptr::eq(retained, original.as_ref()));
        assert!(
            failure.get().is_none(),
            "a raw poll observation does not invent a fatal protocol result"
        );
        failure.observe_io(IoOperation::Read, Arc::new(native_error()));
        assert!(
            Arc::ptr_eq(original, failure.evidence().io_source().unwrap()),
            "only first actual I/O object is retained"
        );
    }
}

#[tokio::test]
async fn actual_hyper_handshake_error_and_original_injected_payload_survive_in_typed_evidence() {
    #[derive(Debug)]
    struct Payload;
    impl std::fmt::Display for Payload {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original injected poll payload")
        }
    }
    impl Error for Payload {}
    let payload = Arc::new(Payload);
    let failure = Arc::new(Failure::new(instance()));
    let io = ObservedIo::new(
        Fault(Some(io::Error::other(payload.clone()))),
        failure.clone(),
    );
    let inbox =
        super::super::super::executor::Inbox::new(NonZeroUsize::new(1).unwrap(), failure.clone());
    let result = crate::split_http::xmux::prepared::http2_with_body::<_, Full<Bytes>, _>(
        io,
        None,
        -1,
        inbox.executor(),
        None,
    )
    .await;
    let hyper = match result {
        Err(source) => source,
        Ok(_) => panic!("original preface write must fail"),
    };
    assert!(
        failure.get().is_none(),
        "only the actual Hyper failure publishes connection failure"
    );
    let first = failure.connection_error(Stage::Handshake, hyper);
    assert!(Arc::ptr_eq(&first, &failure.get().unwrap()));
    let evidence = first
        .source()
        .unwrap()
        .downcast_ref::<H2ConnectionFailure>()
        .unwrap();
    let original = evidence.io_source().unwrap();
    assert!(Arc::ptr_eq(
        original
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<Payload>>()
            .unwrap(),
        &payload
    ));
    let actual = evidence.hyper_source().unwrap();
    let from_chain = evidence
        .source()
        .unwrap()
        .downcast_ref::<hyper::Error>()
        .unwrap();
    assert!(std::ptr::eq(actual.as_ref(), from_chain));
    assert!(
        evidence.h2_source_arc().is_none(),
        "no connection task existed to issue the optional h2 observation"
    );
}

#[tokio::test]
async fn eof_and_pending_reads_never_fabricate_an_original_io_failure() {
    let failure = Arc::new(Failure::new(instance()));
    let mut empty = ObservedIo::new(tokio::io::empty(), failure.clone());
    assert_eq!(empty.read(&mut [0]).await.unwrap(), 0);
    let (io, _held) = tokio::io::duplex(1);
    let mut io = ObservedIo::new(io, failure.clone());
    let mut bytes = [0];
    let mut reading = Box::pin(io.read(&mut bytes));
    pending_once(reading.as_mut()).await;
    drop(reading);
    assert!(failure.get().is_none());
    assert!(failure.evidence().io_source().is_none());
    assert!(failure.evidence().h2_source().is_none());
}
