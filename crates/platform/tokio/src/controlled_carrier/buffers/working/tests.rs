use super::*;
use crate::controlled_carrier::CarrierOriginalTotals;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
};
fn book(bytes: usize) -> CarrierBufferBudget {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 2,
        total_max_bytes: bytes,
    })
    .unwrap();
    root.buffer_budget()
}
fn shape() -> CarrierBufferWorkingShape {
    CarrierBufferWorkingShape {
        receive_scratch_bytes: 2,
        transmit_working_bytes: 2,
        receive_working_bytes: 2,
    }
}
struct Frame {
    data: Vec<u8>,
    _credit: CarrierBufferFrameCredit,
}
fn frame(credit: CarrierBufferFrameCredit) -> Frame {
    let mut data = Vec::new();
    data.try_reserve_exact(credit.bytes()).unwrap();
    assert_eq!(data.capacity(), credit.bytes());
    Frame {
        data,
        _credit: credit,
    }
}
struct Reenter(CarrierBufferBudget, AtomicUsize);
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.used_bytes();
        self.1.fetch_add(1, Ordering::AcqRel);
    }
}
#[test]
fn working_credit_return_progresses_when_the_same_root_has_no_unreserved_bytes() {
    let book = book(6);
    let mut actual = book.try_working_set(shape()).unwrap();
    let scratch = actual.take_receive_scratch().unwrap();
    assert!(actual.take_receive_scratch().is_none());
    let lane = actual.transmit();
    let original = frame(lane.try_reserve(2).unwrap());
    assert_eq!(original.data.capacity(), 2);
    assert_eq!(book.used_bytes(), 6);
    let reenter = Arc::new(Reenter(book.clone(), AtomicUsize::new(0)));
    let waker = Waker::from(reenter.clone());
    let mut cx = Context::from_waker(&waker);
    let returned = {
        let mut waiting = Box::pin(lane.reserve(2));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(original); // Vec is destroyed before the unique working ticket returns.
        assert_eq!(
            book.used_bytes(),
            6,
            "no root refund was needed for progress"
        );
        assert!(reenter.1.load(Ordering::Acquire) > 0);
        let Poll::Ready(Ok(credit)) = waiting.as_mut().poll(&mut cx) else {
            panic!("same working slot must progress")
        };
        assert!(credit.is_working());
        credit
    };
    drop(returned);
    drop(scratch);
    drop(actual);
    drop(lane);
    assert_eq!(book.used_bytes(), 0);
}
#[test]
fn extra_queued_frames_share_the_original_book_and_late_working_backing_stays_charged() {
    let book = book(10);
    let mut actual = book.try_working_set(shape()).unwrap();
    let scratch = actual.take_receive_scratch().unwrap();
    let lane = actual.transmit();
    let working = frame(lane.try_reserve(2).unwrap());
    let extra = frame(lane.try_reserve(3).unwrap());
    assert!(!extra._credit.is_working());
    assert_eq!(book.used_bytes(), 9);
    drop(extra);
    assert_eq!(book.used_bytes(), 6);
    drop(actual);
    assert_eq!(book.used_bytes(), 4);
    drop(lane);
    drop(scratch);
    assert_eq!(book.used_bytes(), 2);
    assert_eq!(working.data.capacity(), 2);
    drop(working);
    assert_eq!(book.used_bytes(), 0);
}
#[test]
fn precise_native_gso_shape_is_not_capped_at_a_borrowed_single_packet() {
    let shape = CarrierBufferWorkingShape {
        receive_scratch_bytes: 65_535,
        transmit_working_bytes: 131_072,
        receive_working_bytes: 65_535,
    };
    let total =
        shape.receive_scratch_bytes + shape.transmit_working_bytes + shape.receive_working_bytes;
    let book = book(total);
    let actual = book.try_working_set(shape).unwrap();
    let lane = actual.transmit();
    assert_eq!(lane.bound(), 131_072);
    let original = frame(lane.try_reserve(131_072).unwrap());
    assert!(original._credit.is_working());
    assert_eq!(original.data.capacity(), 131_072);
    assert_eq!(book.used_bytes(), total);
    drop(original);
    drop(lane);
    drop(actual);
    assert_eq!(book.used_bytes(), 0);
}
#[test]
fn too_small_original_root_refuses_the_whole_peak_without_partial_admission() {
    let book = book(5);
    assert_eq!(
        book.try_working_set(shape()).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(book.used_bytes(), 0);
    let bad = CarrierBufferWorkingShape {
        receive_scratch_bytes: usize::MAX,
        ..shape()
    };
    assert_eq!(
        book.try_working_set(bad).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(book.used_bytes(), 0);
}

#[test]
fn actual_native_bound_growth_wakes_the_same_writer_without_a_root_refund() {
    let book = book(8);
    let actual = book.try_working_set(shape()).unwrap();
    let lane = actual.transmit();
    let wake = Arc::new(Reenter(book.clone(), AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut writer = Box::pin(lane.reserve(4));
    assert!(writer.as_mut().poll(&mut context).is_pending());
    lane.try_grow_bound(4).unwrap();
    assert_eq!(book.used_bytes(), 8);
    assert!(wake.1.load(Ordering::Acquire) > 0);
    let original = match writer.as_mut().poll(&mut context) {
        Poll::Ready(Ok(original)) => original,
        other => panic!("same native bound must progress: {other:?}"),
    };
    assert!(original.is_working());
    assert_eq!(original.bytes(), 4);
    drop(original);
    drop(writer);
    drop(lane);
    drop(actual);
    assert_eq!(book.used_bytes(), 0);
}

#[test]
fn a_larger_hop_keeps_its_outstanding_frame_and_top_up_until_real_return() {
    let book = book(8);
    let actual = book.try_working_set(shape()).unwrap();
    let lane = actual.transmit();
    let old = frame(lane.try_reserve(2).unwrap());
    lane.try_grow_bound(4).unwrap();
    assert_eq!(old.data.capacity(), 2);
    assert_eq!(lane.bound(), 4);
    assert_eq!(book.used_bytes(), 8);
    assert_eq!(
        lane.try_grow_bound(5).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(lane.bound(), 4);
    drop(old);
    let new = lane.try_reserve(4).unwrap();
    assert!(new.is_working());
    assert_eq!(new.bytes(), 4);
    drop(new);
    drop(lane);
    drop(actual);
    assert_eq!(book.used_bytes(), 0);
}
