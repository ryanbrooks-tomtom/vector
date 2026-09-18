use std::time::Duration;

use async_nats::jetstream::{
    AckKind, consumer::pull::Stream as PullConsumerStream, message::Acker,
};
use chrono::Utc;
use futures::{StreamExt, stream::FuturesUnordered};
use snafu::ResultExt;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{DecoderFramedRead, decoding::StreamDecodingError},
    config::{LegacyKey, LogNamespace},
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver},
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, EventsReceived, EventsReceivedHandle,
        InternalEventHandle as _, Protocol,
    },
    lookup::owned_value_path,
};

use crate::{
    SourceSender,
    codecs::Decoder,
    event::Event,
    internal_events::StreamClosedError,
    shutdown::ShutdownSignal,
    sources::nats::config::{BuildError, NatsSourceConfig, SubscribeSnafu},
};

/// The outcome of processing a single NATS message.
pub enum ProcessingStatus {
    /// The message payload was fully decoded and sent downstream.
    Success(Option<BatchStatusReceiver>),
    /// A non-recoverable error occurred while decoding the payload.
    Failed,
    /// The downstream channel is closed, and the source should shut down.
    ChannelClosed,
}

/// Processes a single NATS message, sending decoded events downstream.
///
/// This function contains the common logic for both Core and JetStream NATS.
pub async fn process_message(
    msg: &async_nats::Message,
    config: &NatsSourceConfig,
    decoder: &Decoder,
    log_namespace: LogNamespace,
    out: &mut SourceSender,
    events_received: &EventsReceivedHandle,
    acknowledgements: bool,
) -> ProcessingStatus {
    let mut framed = DecoderFramedRead::new(msg.payload.as_ref(), decoder.clone());
    let mut success = true;
    let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(acknowledgements);

    while let Some(next) = framed.next().await {
        match next {
            Ok((events, _byte_size)) => {
                let count = events.len();
                if count == 0 {
                    continue;
                }

                let byte_size = events.estimated_json_encoded_size_of();
                events_received.emit(CountByteSize(count, byte_size));
                let now = Utc::now();
                let events = events.into_iter().map(|mut event| {
                    if let Event::Log(ref mut log) = event {
                        log_namespace.insert_standard_vector_source_metadata(
                            log,
                            NatsSourceConfig::NAME,
                            now,
                        );
                        let legacy_subject_key_field = config
                            .subject_key_field
                            .path
                            .as_ref()
                            .map(LegacyKey::InsertIfEmpty);
                        log_namespace.insert_source_metadata(
                            NatsSourceConfig::NAME,
                            log,
                            legacy_subject_key_field,
                            &owned_value_path!("subject"),
                            msg.subject.as_str(),
                        );
                    }
                    event.with_batch_notifier_option(&batch)
                });

                if out.send_batch(events).await.is_err() {
                    emit!(StreamClosedError { count });
                    return ProcessingStatus::ChannelClosed;
                }
            }
            Err(error) => {
                success = false;
                // Error is logged by `vector_lib::codecs::Decoder`, no further
                // handling is needed here.
                if !error.can_continue() {
                    break;
                }
            }
        }
    }

    if !success {
        return ProcessingStatus::Failed;
    }

    ProcessingStatus::Success(receiver)
}

pub(crate) fn ack_deadline(ack_wait: Duration, backoff: &[Duration]) -> Duration {
    backoff
        .first()
        .copied()
        .filter(|delay| !delay.is_zero())
        .unwrap_or(ack_wait)
}

fn ack_progress_interval(ack_wait: Duration) -> Duration {
    if ack_wait.is_zero() {
        Duration::from_secs(1)
    } else {
        ack_wait
            .checked_div(2)
            .filter(|delay| !delay.is_zero())
            .unwrap_or(ack_wait)
    }
}

async fn send_progress(acker: &Acker, ack_wait: Duration) {
    let mut progress = tokio::time::interval(ack_progress_interval(ack_wait));
    loop {
        progress.tick().await;
        if let Err(error) = acker.ack_with(AckKind::Progress).await {
            error!(
                message = "Failed to extend JetStream message acknowledgement deadline.",
                %error
            );
        }
    }
}

async fn wait_for_delivery(
    acker: &Acker,
    receiver: &mut BatchStatusReceiver,
    ack_wait: Duration,
) -> BatchStatus {
    tokio::select! {
        status = &mut *receiver => status,
        () = send_progress(acker, ack_wait) => {
            unreachable!("progress acknowledgements never complete");
        }
    }
}

async fn acknowledge(acker: &Acker) {
    if let Err(err) = acker.ack().await {
        error!(message = "Failed to acknowledge JetStream message.", %err);
    }
}

async fn finalize_message(acker: Acker, mut receiver: BatchStatusReceiver, ack_wait: Duration) {
    if wait_for_delivery(&acker, &mut receiver, ack_wait).await == BatchStatus::Delivered {
        acknowledge(&acker).await;
    }
}

fn handle_ack_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        error!(message = "JetStream acknowledgement task failed.", %error);
    }
}

async fn drain_ack_tasks(tasks: &mut FuturesUnordered<tokio::task::JoinHandle<()>>) {
    while let Some(result) = tasks.next().await {
        handle_ack_task_result(result);
    }
}

pub(crate) struct JetStreamAckConfig {
    pub(crate) acknowledgements: bool,
    pub(crate) ack_wait: Duration,
}

pub(crate) async fn run_nats_jetstream(
    config: NatsSourceConfig,
    mut stream: PullConsumerStream,
    ack_config: JetStreamAckConfig,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let JetStreamAckConfig {
        acknowledgements,
        ack_wait,
    } = ack_config;
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));
    let mut finalizers = FuturesUnordered::new();

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => {
                drop(stream);
                drop(out);
                drain_ack_tasks(&mut finalizers).await;
                return Ok(());
            },

            Some(result) = finalizers.next(), if !finalizers.is_empty() => {
                handle_ack_task_result(result);
            }

            maybe_msg = stream.next() => {
                match maybe_msg {
                    Some(Ok(msg)) => {
                        let (msg, acker) = msg.split();
                        bytes_received.emit(ByteSize(msg.payload.len()));

                        let status = tokio::select! {
                            status = process_message(
                                &msg,
                                &config,
                                &decoder,
                                log_namespace,
                                &mut out,
                                &events_received,
                                acknowledgements,
                            ) => status,
                            () = send_progress(&acker, ack_wait) => {
                                unreachable!("progress acknowledgements never complete");
                            }
                        };

                        match status {
                            ProcessingStatus::Success(Some(receiver)) => {
                                finalizers.push(crate::spawn_in_current_span(
                                    finalize_message(acker, receiver, ack_wait)
                                ));
                            }
                            ProcessingStatus::Success(None) => acknowledge(&acker).await,
                            ProcessingStatus::ChannelClosed => return Err(()),
                            // Do not acknowledge on failure; the message will be redelivered.
                            ProcessingStatus::Failed => {}
                        }
                    }
                    Some(Err(error)) => {
                        warn!(message = "JetStream consumer stream error.", %error);
                        break;
                    }
                    None => break,
                };
            }
        }
    }

    drop(out);
    drain_ack_tasks(&mut finalizers).await;
    Ok(())
}

pub async fn run_nats_core(
    config: NatsSourceConfig,
    _connection: async_nats::Client,
    mut subscriber: async_nats::Subscriber,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));

    loop {
        tokio::select! {
            biased;

             _ = &mut shutdown => {
                info!("Shutdown signal received. Draining NATS subscription...");
                if let Err(err) = subscriber.drain().await {
                    error!(message = "Failed to drain NATS subscription.", %err);
                }
            },

            maybe_msg = subscriber.next() => {
                match maybe_msg {
                    Some(msg) => {
                        bytes_received.emit(ByteSize(msg.payload.len()));
                        let status = process_message(
                            &msg,
                            &config,
                            &decoder,
                            log_namespace,
                            &mut out,
                            &events_received,
                            false,
                        )
                        .await;

                        if let ProcessingStatus::ChannelClosed = status {
                            return Err(());
                        }
                    },
                    None => {
                        // The stream has ended. This happens naturally after a successful
                        // drain or if the connection is lost.
                        break;
                    }
                }
            }
        }
    }

    info!("NATS source drained and shut down gracefully.");
    Ok(())
}

pub async fn create_subscription(
    config: &NatsSourceConfig,
) -> Result<(async_nats::Client, async_nats::Subscriber), BuildError> {
    let nc = config.connect().await?;

    let subscription = match &config.queue {
        None => nc.subscribe(config.subject.clone()).await,
        Some(queue) => {
            nc.queue_subscribe(config.subject.clone(), queue.clone())
                .await
        }
    };

    let subscription = subscription.context(SubscribeSnafu)?;

    Ok((nc, subscription))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ack_deadline, ack_progress_interval};

    #[test]
    fn ack_deadline_prefers_first_backoff() {
        assert_eq!(
            ack_deadline(Duration::from_secs(30), &[Duration::from_millis(100)]),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn ack_deadline_ignores_zero_backoff() {
        assert_eq!(
            ack_deadline(Duration::from_secs(30), &[Duration::ZERO]),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn ack_deadline_uses_ack_wait_without_backoff() {
        assert_eq!(
            ack_deadline(Duration::from_secs(30), &[]),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn ack_progress_interval_is_half_deadline() {
        assert_eq!(
            ack_progress_interval(Duration::from_millis(100)),
            Duration::from_millis(50)
        );
    }
}
