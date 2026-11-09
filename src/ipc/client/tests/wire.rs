use super::*;
use crate::ipc::client::wire::Reply;
async fn reply(bytes: &[u8], book: Arc<ControlBudget>) -> Reply {
    let frame = frames::Frame::read(&mut &bytes[..], &book)
        .await
        .unwrap()
        .unwrap();
    Reply::new(frame, book).unwrap()
}
fn new_book(total: usize) -> Arc<ControlBudget> {
    let mut limits = Limits { frame: 4096, total }.budget_limits();
    limits.messages.ipc_frame_bytes = 4096;
    ControlBudget::new(limits)
}
#[tokio::test]
async fn borrowed_formatter_preserves_original_value_wire_and_duplicate_last_semantics() {
    for input in [
        r#"{"z":[1,{"z":"\u00e9","a":"x"}],"a":false,"a":7,"\u0062":null}"#,
        r#"["\ud834\udd1e",-0.0,1e2,18446744073709551615,{"":{}}]"#,
        r#"{"ok":false,"error":{"code":"invalid_argument","message":"bad","details":[{"message":"detail","field_path":null}]}}"#,
    ] {
        let book = new_book(65536);
        let reply = reply(input.as_bytes(), book.clone()).await;
        for pretty in [false, true] {
            let mut actual = Vec::new();
            reply.test_write(&mut actual, pretty).unwrap();
            let expected: serde_json::Value = serde_json::from_str(input).unwrap();
            let expected = if pretty {
                serde_json::to_vec_pretty(&expected).unwrap()
            } else {
                serde_json::to_vec(&expected).unwrap()
            };
            assert_eq!(actual, expected);
            drop(actual);
        }
        drop(reply);
        assert_eq!(book.usage().response_bytes, 0);
    }
}
#[tokio::test]
async fn frame_and_concrete_tun_metadata_are_prepaid_until_the_actual_borrower_returns() {
    let book = new_book(65536);
    let reply=reply(br#"{"ok":true,"result":{"tun_status":{"running":true,"addresses":["10.0.0.1/24","fd00::1/64"],"include_cidrs":["10.0.0.0/8"],"last_error":"owned"}}}"#,book.clone()).await;
    let wire_bytes = book.usage().response_bytes;
    let status = reply.tun_status().unwrap();
    assert!(status.running && status.addresses.len() == 2);
    assert!(book.usage().response_bytes >= wire_bytes + 8 * std::mem::size_of::<String>());
    drop(reply);
    assert!(book.usage().response_bytes > 0);
    assert_eq!(status.last_error.as_deref(), Some("owned"));
    drop(status);
    assert_eq!(book.usage().response_bytes, 0);
}
#[tokio::test]
async fn an_oversized_frame_is_rejected_before_allocation_and_print_workspace_before_output() {
    let book = ControlBudget::new(
        Limits {
            frame: 8,
            total: 64,
        }
        .budget_limits(),
    );
    assert!(frames::Frame::read(&mut &b"123456789\n"[..], &book)
        .await
        .is_err());
    assert_eq!(book.usage().response_bytes, 0);
    let book = new_book(65536);
    let reply = reply(
        br#"{"ok":true,"result":{"many":[{"a":"b"}]}}"#,
        book.clone(),
    )
    .await;
    let mut limits = book.limits();
    limits.messages.response_bytes_total = book.usage().response_bytes;
    book.prepare(limits).unwrap().commit().unwrap();
    assert_eq!(
        reply.print(true, true).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(reply);
    assert_eq!(book.usage().response_bytes, 0);
}
#[tokio::test]
async fn command_error_holds_the_original_frame_and_message_until_its_actual_receiver_drops() {
    let book = new_book(65536);
    let reply = reply(
        br#"{"ok":false,"error":{"code":"timeout","message":"original timeout","details":[]}}"#,
        book.clone(),
    )
    .await;
    let error = match reply.ensure_ok("fallback") {
        Err(error) => error,
        Ok(_) => panic!("must retain wire failure"),
    };
    assert_eq!(error.to_string(), "original timeout");
    assert!(book.usage().response_bytes > 0);
    drop(error);
    assert_eq!(book.usage().response_bytes, 0);
}

#[tokio::test]
async fn unterminated_escape_scratch_stays_paid_with_its_actual_decode_error() {
    let raw = br#"{"ok":true,"result":"\nunterminated"#;
    let book = new_book(65536);
    let frame = frames::Frame::read(&mut &raw[..], &book)
        .await
        .unwrap()
        .unwrap();
    let frame_bytes = book.usage().response_bytes;
    let error = match Reply::new(frame, book.clone()) {
        Ok(_) => panic!("the real incomplete JSON must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(book.usage().response_bytes >= frame_bytes + 2 * b"\\nunterminated".len());
    assert!(error.get_ref().unwrap().source().is_some());
    drop(error);
    assert_eq!(book.usage().response_bytes, 0);
}
