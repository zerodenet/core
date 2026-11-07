use super::*;
use zero_runtime::configuration::ControlBudgetLimits;
fn engine() -> zero_engine::EngineHandle {
    zero_engine::EngineHandle::new(
        zero_engine::Engine::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap(),
    )
}
#[test]
fn each_final_initial_frame_and_ordered_workspace_keep_the_same_root_credit_after_guards_return() {
    let engine = engine();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()));
    for framing in [EventFraming::Ipc, EventFraming::Sse] {
        let (frame, ordered) = prepare_initial_snapshot(&engine, &book, framing, 48).unwrap();
        assert_eq!(book.usage().response_bytes, 0);
        assert_eq!(book.usage().queued_event_bytes, frame.bytes.len() + 48);
        let raw = std::str::from_utf8(&frame.bytes).unwrap();
        let json = match framing {
            EventFraming::Ipc => {
                assert!(raw.ends_with('\n'));
                raw.trim_end()
            }
            EventFraming::Sse => {
                assert!(raw.starts_with("id: "));
                assert!(raw.ends_with("\n\n"));
                raw.split_once("data: ").unwrap().1.trim_end()
            }
        };
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["event_type"], zero_api::event_type::FLOW_SNAPSHOT);
        assert_eq!(value["sequence"], value["payload"]["watermark"]);
        let mut limits = book.limits();
        limits.queued_event_bytes = frame.bytes.len();
        assert_eq!(
            book.prepare(limits).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(ordered);
        book.prepare(limits).unwrap().commit().unwrap();
        let unchanged = frame.bytes.clone();
        let mut session = zero_core::Session::new(
            0,
            zero_core::Address::Domain("later.invalid".into()),
            443,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("test"),
        );
        engine
            .inner()
            .prepare_session(&mut session, "later")
            .unwrap();
        assert_eq!(frame.bytes, unchanged);
        drop(frame);
        assert_eq!(book.usage().queued_event_bytes, 0);
        book.prepare(ControlBudgetLimits::from_runtime(&Default::default()))
            .unwrap()
            .commit()
            .unwrap();
    }
}
#[test]
fn failed_initial_output_admission_releases_every_original_guard_and_keeps_no_byte_debt() {
    let engine = engine();
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.queued_event_bytes = 1;
    let book = ControlBudget::new(limits);
    let error = prepare_initial_snapshot(&engine, &book, EventFraming::Ipc, 0)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(book.usage().queued_event_bytes, 0);
    let mut session = zero_core::Session::new(
        0,
        zero_core::Address::Domain("after-refusal.invalid".into()),
        443,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("test"),
    );
    engine
        .inner()
        .prepare_session(&mut session, "after")
        .unwrap();
}
