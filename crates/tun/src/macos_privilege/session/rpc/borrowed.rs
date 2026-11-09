//! Source inputs are serialized directly into the same prepaid request Box.
//! Only data borrows cross these two synchronous serialization calls; Rpc owns
//! every submitted byte before the original arguments can leave the caller.
use crate::macos_privilege::HelperProgram;

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(in crate::macos_privilege::session) enum BorrowedRequest<'a> {
    OwnedCreateUtun {
        name: Option<&'a str>,
        transfer: u64,
    },
    OwnedCommand {
        program: HelperProgram,
        arguments: &'a [String],
        input: Option<&'a str>,
        nonce: u64,
        output_limit: usize,
    },
}

#[cfg(test)]
mod tests;
