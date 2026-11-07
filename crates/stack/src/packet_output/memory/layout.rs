use zero_traits::PacketDeriveError;
/// Immutable byte transformations of the original wire codec. The protocol
/// supplies these sizes; they do not select a peer or authorize any IO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketWireMemory {
    Native,
    PaddedEnvelope { multiple: u16, overhead: u16 },
}
impl PacketWireMemory {
    pub fn workspace_lengths(self, length: usize) -> Result<Vec<usize>, PacketDeriveError> {
        match self {
            Self::Native => Ok(Vec::new()),
            Self::PaddedEnvelope { multiple, overhead } => {
                let multiple = usize::from(multiple);
                if multiple == 0 {
                    return Err(PacketDeriveError::LayoutMismatch);
                }
                let padded = length
                    .checked_add(multiple - 1)
                    .ok_or(PacketDeriveError::Overflow)?
                    / multiple
                    * multiple;
                let encoded = padded
                    .checked_add(usize::from(overhead))
                    .ok_or(PacketDeriveError::Overflow)?;
                // Padding occupies the already admitted cipher payload. The
                // actual codec needs only this additional output allocation.
                Ok(vec![encoded])
            }
        }
    }
}

impl PacketWireMemory {
    /// Finite same-family upper bound before a response's bytes exist. These
    /// are promises on the original envelope, not another buffer allocator.
    pub fn return_workspace_lengths(
        self,
        maximum: usize,
        mtu: usize,
        ipv4: bool,
    ) -> Result<Vec<usize>, PacketDeriveError> {
        if maximum <= mtu {
            return self.workspace_lengths(maximum);
        }
        let header = if ipv4 { 60 } else { 48 };
        let chunk = mtu
            .checked_sub(header)
            .map(|n| n / 8 * 8)
            .filter(|n| *n != 0)
            .ok_or(PacketDeriveError::LayoutMismatch)?;
        let count = maximum.div_ceil(chunk);
        let mut lengths = Vec::new();
        for _ in 0..count {
            lengths.push(mtu);
            lengths.extend(self.workspace_lengths(mtu)?);
        }
        Ok(lengths)
    }
}
