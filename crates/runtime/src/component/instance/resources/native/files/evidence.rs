use super::*;
use zero_platform_tokio::file::FileReadEvidence;

/// A joined blocking job and a closed native descriptor are independent facts.
#[derive(Clone, Debug)]
pub(in crate::component) struct FileIoEvidence {
    pub path: PathBuf,
    pub scheduled: bool,
    pub job_joined: bool,
    pub read: FileReadEvidence,
}
impl FileIoEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        (!self.scheduled || self.job_joined)
            && matches!(
                self.read.native,
                FileNativeEvidence::KnownNoAcquisition | FileNativeEvidence::ConfirmedClosed
            )
    }
}
impl FileReads {
    pub(in super::super) fn evidence(&self) -> Vec<FileIoEvidence> {
        self.grants
            .iter()
            .map(|(path, grant)| FileIoEvidence {
                path: path.clone(),
                scheduled: grant.pending.is_some(),
                job_joined: grant.joined,
                read: grant
                    .pending
                    .as_ref()
                    .map(|owner| owner.evidence())
                    .unwrap_or(FileReadEvidence {
                        native: FileNativeEvidence::KnownNoAcquisition,
                        read_source: None,
                        close_source: None,
                    }),
            })
            .collect()
    }
}
