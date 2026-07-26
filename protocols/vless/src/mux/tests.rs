use super::backlog::MuxResponseBacklogPolicy;
use super::VlessInboundMuxWriter;

#[test]
fn inbound_writer_uses_configured_frame_limit() {
    let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16 * 1024))
        .expect("valid VLESS MUX response backlog policy");
    let (writer, _responses) = VlessInboundMuxWriter::channel(policy);

    writer.data(1, vec![1]).expect("first response frame");
    let error = writer
        .data(2, vec![2])
        .expect_err("second response frame must exceed configured capacity");
    assert!(error.to_string().contains("frame limit"));
}

#[test]
fn inbound_writer_uses_configured_byte_limit() {
    let policy = MuxResponseBacklogPolicy::from_config(Some(2), Some(16 * 1024))
        .expect("valid VLESS MUX response backlog policy");
    let (writer, _responses) = VlessInboundMuxWriter::channel(policy);

    writer
        .data(1, vec![0; 16 * 1024])
        .expect("response at configured byte limit");
    let error = writer
        .data(2, vec![1])
        .expect_err("response beyond configured byte limit must fail");
    assert!(error.to_string().contains("byte limit"));
}
