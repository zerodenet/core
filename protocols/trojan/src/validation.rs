pub const DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 32;
pub const DEFAULT_MUX_RESPONSE_BACKLOG_BYTES: u64 = 1024 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 4096;
pub const MIN_MUX_RESPONSE_BACKLOG_BYTES: u64 = 16 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MuxResponseBacklogPolicy {
    frames: usize,
    bytes: usize,
}

pub fn validate_mux_response_backlog(
    frames: Option<u32>,
    bytes: Option<u64>,
) -> Result<(), &'static str> {
    if frames.is_some_and(|value| value == 0 || value > MAX_MUX_RESPONSE_BACKLOG_FRAMES) {
        return Err("Trojan MUX response backlog frames must be within 1..=4096");
    }
    if bytes.is_some_and(|value| {
        !(MIN_MUX_RESPONSE_BACKLOG_BYTES..=MAX_MUX_RESPONSE_BACKLOG_BYTES).contains(&value)
    }) {
        return Err("Trojan MUX response backlog bytes must be within 16384..=67108864");
    }
    Ok(())
}

impl MuxResponseBacklogPolicy {
    pub(crate) fn from_config(
        frames: Option<u32>,
        bytes: Option<u64>,
    ) -> Result<Self, &'static str> {
        validate_mux_response_backlog(frames, bytes)?;
        Ok(Self {
            frames: frames.unwrap_or(DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES) as usize,
            bytes: bytes.unwrap_or(DEFAULT_MUX_RESPONSE_BACKLOG_BYTES) as usize,
        })
    }

    pub(crate) const fn frames(self) -> usize {
        self.frames
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }
}

impl Default for MuxResponseBacklogPolicy {
    fn default() -> Self {
        Self {
            frames: DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES as usize,
            bytes: DEFAULT_MUX_RESPONSE_BACKLOG_BYTES as usize,
        }
    }
}
