use zero_engine::EngineError;

fn unsupported_io(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        message,
    ))
}

pub(in crate::protocol_registry) fn relay_hop_unsupported() -> EngineError {
    unsupported_io("this adapter does not support relay hop")
}
