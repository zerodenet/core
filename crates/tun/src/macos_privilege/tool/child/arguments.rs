//! Only borrowed argv is projected into the original fixed Command.
use std::process::Command;
pub(super) enum Arguments<'a> {
    Strings(&'a [String]),
    Slices(&'a [&'a str]),
}
impl<'a> From<&'a [String]> for Arguments<'a> {
    fn from(arguments: &'a [String]) -> Self {
        Self::Strings(arguments)
    }
}
impl Arguments<'_> {
    pub(super) fn append_to(self, command: &mut Command) {
        match self {
            Self::Strings(arguments) => command.args(arguments),
            Self::Slices(arguments) => command.args(arguments),
        };
    }
}
#[cfg(test)]
mod tests;
