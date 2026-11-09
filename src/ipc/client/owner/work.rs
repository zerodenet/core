//! Install the real Scope IO task and persistent driver before releasing gates.
use super::*;
use std::num::NonZeroUsize;
use tokio::io::{AsyncWriteExt, BufReader};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
impl Attempt {
    pub(in crate::ipc::client) async fn request(
        self: &Arc<Self>,
        path: &str,
        input: frames::Frame,
        events: bool,
    ) -> io::Result<Option<wire::Reply>> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other("client attempt already submitted"));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        #[cfg(windows)]
        let limit = 2;
        #[cfg(not(windows))]
        let limit = 1;
        let mut scope = TaskScope::suspended_hosted(runtime.clone(), NonZeroUsize::new(limit));
        scope.enable_registration().map_err(io::Error::other)?;
        let registrar = scope.registrar();
        *self.scope.lock().await = Some(scope);
        let opening =
            self.native
                .lock()
                .await
                .prepare(path, registrar.clone(), self.stop.clone())?;
        let driver_slot = self.driver.reserve()?;
        let (gate, ready) = oneshot::channel();
        let (reply, received) = oneshot::channel();
        let original = self.clone();
        let registered = registrar.spawn_tracked(TaskClass::Auxiliary, async move {
            if ready.await.is_err() {
                return Ok(());
            }
            let result = original.io(opening, input, events).await;
            #[cfg(test)]
            if original.fatal_decode.swap(false, Ordering::AcqRel) {
                if let Err(error) = result {
                    return Err(zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Receive,
                        zero_platform_tokio::contract::Reason::Internal,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(error));
                }
            }
            if let Err(result) = reply.send(result) {
                if let Err(error) = result {
                    lock(&original.faults.abandoned).get_or_insert_with(|| Arc::new(error));
                }
            }
            Ok(())
        });
        let io_id = registered.as_ref().ok().map(|(id, _)| *id);
        let admitted = registered.is_ok();
        if let Err(error) = registered {
            // The cold controller and real Scope are still installed. This
            // same driver performs cleanup even when registration failed.
            lock(&self.faults.abandoned).get_or_insert_with(|| Arc::new(io::Error::other(error)));
        }
        let original = self.clone();
        let (driver_gate, driver_ready) = oneshot::channel();
        let driver = runtime.spawn(async move {
            if driver_ready.await.is_err() {
                return Err(io::Error::other(
                    "client driver startup gate was not published",
                ));
            }
            if admitted {
                let _ = gate.send(());
            } else {
                drop(gate);
            }
            original.drive(io_id).await
        });
        driver_slot.install(driver);
        let _ = driver_gate.send(());
        match received.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(
                "original client IO task did not return a reply",
            )),
        }
    }
    async fn io(
        self: &Arc<Self>,
        opening: native::Opening,
        input: frames::Frame,
        events: bool,
    ) -> io::Result<Option<wire::Reply>> {
        let mut stream = tokio::select! {biased;_=self.stop.cancelled()=>return Err(stopped()),result=opening.connect()=>result?};
        tokio::select! {biased;_=self.stop.cancelled()=>return Err(stopped()),result=async{stream.write_all(&input.bytes).await?;stream.flush().await}=>result?};
        drop(input);
        let workspace = (self.book.limits().messages.response_bytes_total / 4).clamp(1, 8192);
        let credit = self.book.reserve(ControlCharge::ResponseBytes(workspace))?;
        let mut reader = BufReader::with_capacity(
            workspace,
            PendingRead {
                stream,
                #[cfg(test)]
                original: self.clone(),
            },
        );
        loop {
            let frame = tokio::select! {biased;_=self.stop.cancelled()=>return Err(stopped()),result=frames::Frame::read(&mut reader,&self.book)=>result?};
            let Some(frame) = frame else {
                drop(reader);
                drop(credit);
                return Ok(None);
            };
            if matches!(frame.bytes.as_slice(), b"\n" | b"\r\n") {
                continue;
            }
            let reply = wire::Reply::new(frame, self.book.clone())?;
            if events {
                reply.print(false, false)?;
            } else {
                drop(reader);
                drop(credit);
                return Ok(Some(reply));
            }
        }
    }
    async fn drive(
        self: Arc<Self>,
        io_id: Option<zero_platform_tokio::contract::scope::TaskId>,
    ) -> io::Result<()> {
        {
            let mut guard = self.scope.lock().await;
            let scope = guard
                .as_mut()
                .expect("actual Scope installed before driver");
            while io_id.is_some() {
                let receipt = tokio::select! {biased;_=self.stop.cancelled()=>break,receipt=scope.next_receipt()=>receipt};
                let Some(receipt) = receipt else { break };
                let io_done = Some(receipt.id()) == io_id;
                self.accept(scope, receipt)?;
                // The Windows std-join wrapper may finish while the original
                // IO task is still consuming its final buffered response.
                if io_done {
                    break;
                }
            }
        }
        self.stop.cancel();
        let end = *self.end.get_or_init(crate::ipc::owner::close_deadline);
        self.progress(end).await?;
        if !self.retired.load(Ordering::Acquire) {
            return Err(self.incomplete("original client cleanup remains retained"));
        }
        self.faults.report()
    }
}
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "control client cancelled")
}
struct PendingRead {
    stream: native::Stream,
    #[cfg(test)]
    original: Arc<Attempt>,
}
impl tokio::io::AsyncRead for PendingRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let result = std::pin::Pin::new(&mut self.stream).poll_read(cx, buffer);
        #[cfg(test)]
        if result.is_pending() {
            self.original.read_pending.store(true, Ordering::Release);
            self.original.changed.notify_waiters();
            if self.original.panic_pending.swap(false, Ordering::AcqRel) {
                panic!("actual client IO fault after underlying Pending");
            }
        }
        result
    }
}
