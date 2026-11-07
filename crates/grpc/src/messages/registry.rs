use std::sync::{Arc, Mutex, OnceLock};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_runtime::configuration::{
    ControlBudget, ControlBudgetPermit, ControlCharge, ControlConnectionBudget,
    ControlConnectionCharge, ControlConnectionPermit, ControlMessageLimits,
};

pub(crate) struct Record {
    witness: OnceLock<TaskJoinWitness>,
    pub(super) lifetime: Arc<Lifetime>,
    pub(super) budget: Arc<ControlBudget>,
    pub(super) policy: ControlMessageLimits,
    input: Mutex<Option<ControlBudgetPermit>>,
    pub(super) output: super::output::Outbox,
    pub(super) command: super::command::Command,
    pub(super) status: Mutex<Option<ControlBudgetPermit>>,
}
pub(super) struct Lifetime {
    _permit: ControlConnectionPermit,
    pub(super) encoder: Mutex<(ControlBudgetPermit, usize)>,
}
impl Record {
    pub(super) fn start_input(&self) -> Result<(), tonic::Status> {
        let mut input = self
            .input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if input.is_some() {
            return Err(tonic::Status::internal("duplicate original gRPC input"));
        }
        // This is the actual pinned Tonic decoder's existing buffer geometry,
        // not a larger production buffer or a second decoder.
        *input = Some(
            self.budget
                .reserve(ControlCharge::RequestBytes(super::CODEC_BUFFER_SIZE))
                .map_err(|error| tonic::Status::resource_exhausted(error.to_string()))?,
        );
        Ok(())
    }
    pub(super) fn input_message(&self, bytes: usize) -> Result<(), tonic::Status> {
        if bytes > self.policy.grpc_request_bytes {
            return Err(tonic::Status::out_of_range(
                "gRPC request exceeds its configured decoder limit",
            ));
        }
        // Decoder growth (at most twice the frame), protobuf payload and JSON
        // payload are separate storage bounds. Reserve before Tonic receives the
        // complete length header and can grow its original decoder buffer.
        let bytes = bytes
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(5))
            .ok_or_else(|| tonic::Status::resource_exhausted("gRPC input length overflow"))?;
        self.input
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            .ok_or_else(|| tonic::Status::internal("original gRPC input not installed"))?
            .grow(bytes)
            .map_err(|error| tonic::Status::resource_exhausted(error.to_string()))
    }
    pub(super) fn bind(&self, witness: TaskJoinWitness) -> bool {
        if let Some(actual) = self.witness.get() {
            return actual.same_registration(&witness);
        }
        match self.witness.set(witness) {
            Ok(()) => true,
            Err(witness) => self
                .witness
                .get()
                .is_some_and(|actual| actual.same_registration(&witness)),
        }
    }
    pub(super) fn command_registration(&self) -> Option<TaskJoinWitness> {
        self.witness.get().cloned()
    }
    fn released(&self) -> bool {
        !self.command.unknown()
            && self.witness.get().is_some_and(|actual| {
                actual.try_joined().is_some_and(|joined| {
                    actual.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
}
pub(crate) struct Requests {
    pub(super) budget: Arc<ControlBudget>,
    originals: Mutex<Vec<Arc<Record>>>,
}
impl Requests {
    pub(crate) fn new(budget: Arc<ControlBudget>) -> Self {
        Self {
            budget,
            originals: Mutex::new(Vec::new()),
        }
    }
    pub(super) fn reserve(
        &self,
        connection: &ControlConnectionBudget,
    ) -> std::io::Result<Arc<Record>> {
        let original = Arc::new(Record {
            witness: OnceLock::new(),
            lifetime: Arc::new(Lifetime {
                _permit: connection.reserve(ControlConnectionCharge::GrpcStream)?,
                encoder: Mutex::new((
                    self.budget
                        .reserve(ControlCharge::ResponseBytes(super::CODEC_BUFFER_SIZE))?,
                    super::CODEC_BUFFER_SIZE,
                )),
            }),
            budget: self.budget.clone(),
            policy: self.budget.limits().messages,
            input: Mutex::new(None),
            output: Default::default(),
            command: Default::default(),
            status: Mutex::new(None),
        });
        self.originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(original.clone());
        Ok(original)
    }
    /// Only the actual synchronous spawn Err may remove an unregistered row.
    pub(super) fn rejected(&self, original: &Arc<Record>) {
        let removed = {
            let mut entries = self
                .originals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .iter()
                .position(|actual| Arc::ptr_eq(actual, original))
                .map(|index| entries.swap_remove(index))
        };
        drop(removed);
    }
    /// Pure observation of actual joins and ACK; this never polls a Scope.
    pub(crate) fn reap(&self) {
        let removed = {
            let mut entries = self
                .originals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut removed = Vec::new();
            let mut index = 0;
            while index < entries.len() {
                if entries[index].released() {
                    removed.push(entries.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            removed
        };
        drop(removed);
    }
    pub(crate) fn confirm_released(&self) -> std::io::Result<()> {
        self.reap();
        if self
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            Ok(())
        } else {
            Err(std::io::Error::other(
                "original gRPC request/command cleanup remains unconfirmed",
            ))
        }
    }
}
