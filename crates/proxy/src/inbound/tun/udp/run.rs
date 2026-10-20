//! The shared UDP loop; raw-IP supplies persistent child sets.
use super::*;

pub(super) async fn run_inner(
    runtime: UdpIngressRuntime,
    stack: Arc<UserUdpStack>,
    inbound_tag: String,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
    #[cfg(feature = "raw-ip-runtime")] owned: Option<OwnedTunUdpTasks>,
) -> Result<(), EngineError> {
    let mut buffer = vec![0_u8; 65_535];
    let mut next_id = 1_u64;
    let mut associations = AssociationRegistry::new();
    let mut tasks = JoinSet::new();
    let mut dns_tasks = JoinSet::new();
    #[cfg(feature = "raw-ip-runtime")]
    let mut owned_tasks = match &owned {
        Some(owner) => Some(
            owner
                .associations
                .lock()
                .await
                .map_err(std::io::Error::other)?,
        ),
        None => None,
    };
    #[cfg(feature = "raw-ip-runtime")]
    let mut owned_dns = match &owned {
        Some(owner) => Some(owner.dns.lock().await.map_err(std::io::Error::other)?),
        None => None,
    };
    #[cfg(feature = "raw-ip-runtime")]
    let tasks = owned_tasks.as_deref_mut().unwrap_or(&mut tasks);
    #[cfg(not(feature = "raw-ip-runtime"))]
    let tasks = &mut tasks;
    #[cfg(feature = "raw-ip-runtime")]
    let dns_tasks = owned_dns.as_deref_mut().unwrap_or(&mut dns_tasks);
    #[cfg(not(feature = "raw-ip-runtime"))]
    let dns_tasks = &mut dns_tasks;
    let mut last_dns_pressure_log = None;

    loop {
        tokio::select! {
            received = stack.recv_from_with_peer(&mut buffer) => {
                let Some((size, source, destination, peer_identity)) = received else {
                    return Ok(());
                };
                if dns_hijack && destination.port == 53 {
                    dns_hijacked_queries.fetch_add(1, Ordering::Relaxed);
                    let now = Instant::now();
                    if dns_tasks.len() >= MAX_CONCURRENT_DNS_QUERIES {
                        if pressure_log_due(&mut last_dns_pressure_log, now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                active_dns_queries = dns_tasks.len(),
                                "dropping TUN DNS query at the concurrency limit"
                            );
                        }
                        let response = runtime.resolver().busy_response(&buffer[..size]);
                        stack.send_to(&response, destination, source).await;
                        continue;
                    }
                    let query_runtime = runtime.clone();
                    let stack = Arc::clone(&stack);
                    let query = buffer[..size].to_vec();
                    dns_tasks.spawn(async move {
                        let response = query_runtime.resolver().answer_udp_query(&query).await?;
                        stack.send_to(&response, destination, source).await;
                        Ok::<(), std::io::Error>(())
                    });
                    continue;
                }
                let datagram = TunDatagram {
                    peer_identity,
                    destination,
                    payload: buffer[..size].to_vec(),
                };
                let now = Instant::now();
                tracing::trace!(
                    ?source,
                    ?destination,
                    association_id = associations.association_id(source),
                    active_associations = associations.active_count(),
                    "TUN UDP ingress"
                );
                match associations.deliver(source, datagram) {
                    Delivery::Delivered => {}
                    Delivery::Missing(datagram) => {
                        #[cfg(feature = "raw-ip-runtime")]
                        if owned.is_some() && tasks.len() >= association::MAX_ACTIVE_ASSOCIATIONS {
                            log_pressure_drop(&mut associations, now, source, datagram.destination,
                                AdmissionRejection::ActiveLimit,
                                stack.send_unreachable(&datagram.payload, source, datagram.destination));
                            continue;
                        }
                        match associations.admit(source, now) {
                            Ok(()) => {
                                let id = next_id;
                                spawn_association(
                                    tasks,
                                    &mut associations,
                                    AssociationStart {
                                        runtime: runtime.clone(),
                                        stack: Arc::clone(&stack),
                                        inbound_tag: inbound_tag.clone(),
                                        source,
                                        id,
                                        first: datagram,
                                    },
                                );
                                tracing::trace!(
                                    ?source,
                                    ?destination,
                                    association_id = id,
                                    active_associations = associations.active_count(),
                                    "TUN UDP association started"
                                );
                                next_id = next_id.wrapping_add(1).max(1);
                            }
                            Err(reason) => log_pressure_drop(
                                &mut associations,
                                now,
                                source,
                                datagram.destination,
                                reason,
                                stack.send_unreachable(&datagram.payload, source, datagram.destination),
                            ),
                    }
                    },
                    Delivery::Full(datagram) => {
                        let feedback_sent = stack.send_unreachable(
                            &datagram.payload,
                            source,
                            datagram.destination,
                        );
                        if associations.should_log_pressure(now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                feedback_sent,
                                active_associations = associations.active_count(),
                                "dropping TUN UDP datagram because its association queue is full"
                            );
                        }
                    }
                    Delivery::Closed(datagram) => {
                        let feedback_sent = stack.send_unreachable(
                            &datagram.payload,
                            source,
                            datagram.destination,
                        );
                        if associations.remove(source) {
                            associations.record_failure(source, now);
                        }
                        if associations.should_log_pressure(now) {
                            tracing::warn!(
                                ?source,
                                ?destination,
                                feedback_sent,
                                active_associations = associations.active_count(),
                                "TUN UDP association closed; applying recreate backoff"
                            );
                        }
                    }
                }
            }
            Some(completed) = tasks.join_next(), if !tasks.is_empty() => {
                match completed {
                    Ok((source, id, Ok(()))) => {
                        if associations.remove_matching(source, id) {
                            associations.clear_failure(source);
                        }
                    }
                    Ok((source, id, Err(error))) => {
                        if associations.remove_matching(source, id) {
                            associations.record_failure(source, Instant::now());
                        }
                        tracing::warn!(error = %error, ?source, "TUN UDP association failed");
                    }
                    Err(error) => {
                        #[cfg(feature = "raw-ip-runtime")]
                        if let Some(owner) = &owned {
                            return Err(std::io::Error::other(owner.associations.remember(error)).into());
                        }
                        tracing::warn!(error = %error, "TUN UDP association task panicked");
                    },
                }
            }
            Some(completed) = dns_tasks.join_next(), if !dns_tasks.is_empty() => {
                match completed {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(error = %error, "TUN DNS query failed"),
                    Err(error) => {
                        #[cfg(feature = "raw-ip-runtime")]
                        if let Some(owner) = &owned {
                            return Err(std::io::Error::other(owner.dns.remember(error)).into());
                        }
                        tracing::warn!(error = %error, "TUN DNS task panicked");
                    },
                }
            }
        }
    }
}
