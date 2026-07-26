use std::sync::mpsc::{self, TrySendError};
use std::thread;

use zero_api::RawApiEvent;

use super::{outbox, PendingDelivery};
use crate::registry::ConfiguredEventSink;

pub(super) type SinkPublishResult = Result<zero_api::PublishResult, zero_api::ApiError>;

pub(super) struct SinkWorkerResult {
    pub(super) sink_tag: String,
    pub(super) event_id: String,
    pub(super) result: SinkPublishResult,
}

pub(super) struct SinkWorker {
    pub(super) tag: String,
    event_types: Vec<String>,
    source_id: Option<String>,
    sender: Option<mpsc::SyncSender<RawApiEvent>>,
    task: Option<thread::JoinHandle<()>>,
    pub(super) in_flight: Option<PendingDelivery>,
}

impl SinkWorker {
    pub(super) fn spawn(
        sink: ConfiguredEventSink,
        result_tx: mpsc::Sender<SinkWorkerResult>,
    ) -> Self {
        let tag = sink.tag.clone();
        let event_types = sink.event_types.clone();
        let source_id = sink.source_id.clone();
        let worker_tag = tag.clone();
        let (sender, receiver) = mpsc::sync_channel::<RawApiEvent>(1);
        let task = thread::Builder::new()
            .name(format!("zero-event-sink-{tag}"))
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    let event_id = event.event_id.clone();
                    let result = sink.publish_prepared(&event);
                    if result_tx
                        .send(SinkWorkerResult {
                            sink_tag: worker_tag.clone(),
                            event_id,
                            result,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .expect("event sink worker thread");
        Self {
            tag,
            event_types,
            source_id,
            sender: Some(sender),
            task: Some(task),
            in_flight: None,
        }
    }

    pub(super) fn accepts(&self, event: &RawApiEvent) -> bool {
        self.event_types.is_empty()
            || self
                .event_types
                .iter()
                .any(|event_type| event_type == &event.event_type)
    }

    pub(super) fn prepare_event(&self, event: &RawApiEvent) -> RawApiEvent {
        let mut event = event.clone();
        if let Some(source_id) = &self.source_id {
            event.source_id = Some(source_id.clone());
        }
        event
    }

    pub(super) fn try_submit(&mut self, delivery: PendingDelivery) -> Option<PendingDelivery> {
        if self.in_flight.is_some() {
            return Some(delivery);
        }
        let Some(sender) = self.sender.as_ref() else {
            return Some(delivery);
        };
        match sender.try_send(delivery.event.clone()) {
            Ok(()) => {
                self.in_flight = Some(delivery);
                None
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => Some(delivery),
        }
    }

    pub(super) fn in_flight_key(&self) -> Option<outbox::DeliveryKey> {
        self.in_flight.as_ref().map(PendingDelivery::key)
    }

    pub(super) fn stop(&mut self) {
        self.sender.take();
    }

    pub(super) fn join(&mut self) {
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}
