use super::*;
use std::future::Future;
pub struct Rx {
    shared: Arc<Logical>,
    association: AssociationId,
    cancel: BoxFuture<'static, ()>,
}
impl Rx {
    pub(super) fn new(shared: Arc<Logical>, association: AssociationId) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            association,
        }
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        this.shared.check(&mut this.cancel, cx, Stage::Receive)?;
        let outcome = {
            let mut state = this.shared.state.lock().unwrap();
            state.receiver = Some(cx.waker().clone());
            let mut ready = None;
            for (index, pending) in state.pending.iter_mut().enumerate() {
                if let Poll::Ready(outcome) = Pin::new(pending).poll(cx) {
                    ready = Some((index, outcome));
                    break;
                }
            }
            ready.map(|(index, outcome)| {
                let original = state.pending.remove(index);
                (original, outcome)
            })
        };
        let Some((returned, outcome)) = outcome else {
            return Poll::Pending;
        };
        drop(returned);
        match outcome {
            Ok((address, port, payload)) => Poll::Ready(Ok(Some(Datagram {
                association: this.association,
                address: zero_core::request::Destination::new(address, port),
                payload: payload.into(),
            }))),
            Err(source) => Poll::Ready(Err(this
                .shared
                .error(Stage::Receive, Reason::Io, DeliveryState::Unknown)
                .with_source(source))),
        }
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}
