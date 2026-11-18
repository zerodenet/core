//! Exercise a real native loan through retained framing controls, as the
//! production taskless Stream/Datagram adapter does before cache adoption.
use crate::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl, Unfinished},
        BoxFuture, ChannelError, CloseReason,
    },
    controlled_carrier::loan::{LoanOwner, StreamLoan},
};
use std::sync::Arc;
use tokio::{sync::Mutex, time::Instant};

struct Framing(Arc<Mutex<Option<LoanOwner>>>);
impl DynChannelControl for Framing {
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        let Ok(mut original) = self.0.try_lock() else {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "actual input controller is busy".into(),
                )],
                ..Default::default()
            });
        };
        original.as_mut().expect("same actual input").try_close()
    }
    fn cancel(&self, reason: CloseReason) {
        if let Ok(original) = self.0.try_lock() {
            original.as_ref().expect("same actual input").cancel(reason);
        }
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0
                .lock()
                .await
                .as_mut()
                .expect("same actual input")
                .close(end)
                .await
        })
    }
}
pub(super) async fn wrap(loan: StreamLoan) -> StreamLoan {
    let original = loan.owner.original().clone();
    let input = Arc::new(Mutex::new(Some(loan.owner)));
    let mut framed = StreamLoan::new(
        Box::pin(loan.io),
        Box::new(Framing(input.clone())).into(),
        original,
    );
    framed.owner.retain_input_owner(input).await.unwrap();
    framed
}
