use super::super::{observed_io::ObservedIo, H2ConnectionFailure, IoOperation};
use super::*;
use std::io;
use zero_platform_tokio::contract::{DeliveryState, Impact, RetryClass};

fn fields(
    error: &ChannelError,
) -> (
    Option<ComponentId>,
    Option<zero_core::component::SessionId>,
    Stage,
    Reason,
    Impact,
    DeliveryState,
    RetryClass,
) {
    (
        error.component,
        error.session,
        error.stage,
        error.reason,
        error.impact,
        error.delivery,
        error.retry,
    )
}
async fn finish(
    fixture: &mut Fixture,
    scope: &mut TaskScope,
    adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    id: TaskId,
) {
    assert_eq!(scope.next_exit().await.unwrap().id, id);
    assert!(
        adoption.closed().is_none(),
        "aggregate join does not close the native descriptor"
    );
    assert_eq!(fixture.owner.retained(), 1);
    assert!(adoption.retire(end()).await.unwrap().is_complete());
    assert_eq!(adoption.closed().unwrap().driver_exit().unwrap().id(), id);
    assert_eq!(fixture.owner.retained(), 0);
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
}
mod physical;
mod poll;
mod reset;
