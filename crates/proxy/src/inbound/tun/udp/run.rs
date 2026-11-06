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
    let mut legacy_tasks = JoinSet::new();
    let mut legacy_dns = JoinSet::new();
    #[cfg(feature = "raw-ip-runtime")]
    let admission = match &owned {
        Some(owner) => Some(owner.source_admission().await?),
        None => None, // Explicit library/test executor, outside a hosted Source.
    };
    #[cfg(feature = "raw-ip-runtime")]
    let mut tasks = match &owned {
        Some(owner) => task_set::TunTasks::Scoped(super::scoped::ScopedTasks {
            scope: owner.scope.get().cloned().ok_or_else(|| {
                std::io::Error::other("actual raw-IP association Scope was not installed")
            })?,
            completed: owner.completed.clone(),
            lost: super::owned::lost_completion,
            receiver: owner.receiver.lock().await,
        }),
        None => task_set::TunTasks::Legacy(&mut legacy_tasks),
    };
    #[cfg(not(feature = "raw-ip-runtime"))]
    let mut tasks = task_set::TunTasks::Legacy(&mut legacy_tasks);
    #[cfg(feature = "raw-ip-runtime")]
    let mut dns_tasks = match &owned {
        Some(owner) => {
            task_set::TunTasks::Owned(owner.dns.lock().await.map_err(std::io::Error::other)?)
        }
        None => task_set::TunTasks::Legacy(&mut legacy_dns),
    };
    #[cfg(not(feature = "raw-ip-runtime"))]
    let mut dns_tasks = task_set::TunTasks::Legacy(&mut legacy_dns);
    let mut last_dns_pressure_log = None;

    loop {
        tokio::select! {
            received = async {
                #[cfg(feature = "raw-ip-runtime")]
                if let Some(admission) = &admission {
                    let (received, acquisition) = crate::runtime::raw_ip::tasks::input::admitted(
                        stack.recv_from_with_peer(&mut buffer), admission,
                    ).await.map_err(std::io::Error::other)?;
                    return Ok::<_, EngineError>((received, Some(acquisition)));
                }
                let received = stack.recv_from_with_peer(&mut buffer).await;
                #[cfg(feature = "raw-ip-runtime")]
                let acquisition = None;
                #[cfg(not(feature = "raw-ip-runtime"))]
                let acquisition = ();
                Ok::<_, EngineError>((received, acquisition))
            } => {
                // Retain the actual dequeue acquisition across reserve and
                // synchronous spawn_bound/spawn. The same child records own
                // every admitted datagram before this branch releases it.
                let (received, _acquisition) = received?;
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
                    let query_stack = Arc::clone(&stack);
                    let query = buffer[..size].to_vec();
                    #[cfg(feature = "raw-ip-runtime")]
                    let mut native = match &owned {
                        Some(owner) => match owner.native.reserve_reclaimed(
                            crate::runtime::inbound_operation::raw_ip::native_obligations::ChildKind::Dns,
                            tokio::time::Instant::now() + Duration::from_secs(5),
                        ).await {
                            Ok(native) => Some(native),
                            Err(error) if crate::runtime::raw_ip::tasks::capacity::before_work(&error) => {
                                let response = runtime.resolver().busy_response(&buffer[..size]);
                                stack.send_to(&response, destination, source).await;
                                continue;
                            }
                            Err(error) => return Err(std::io::Error::other(error).into()),
                        },
                        None => None,
                    };
                    let registered = dns_tasks.spawn_with_witness(move |witness| async move {
                        let resolver = query_runtime.resolver();
                        let (call, work) = resolver.prepare_udp_query(&query);
                        #[cfg(feature = "raw-ip-runtime")]
                        if let Some(native) = &mut native {
                            let witness = witness.ok_or_else(|| std::io::Error::other("DNS child has no actual Source registration"))?;
                            native.bind_dns_call(call,
                                crate::runtime::inbound_operation::raw_ip::native_obligations::DnsSourceCaller::DatagramChild(witness),
                            ).map_err(std::io::Error::other)?;
                        }
                        #[cfg(not(feature = "raw-ip-runtime"))]
                        let _ = (call, witness);
                        let response = work.await;
                        // The original Auxiliary output keeps query failures;
                        // the actual call token separately proves cleanup.
                        // A normal failed query cannot poison all later calls
                        // after its matching originals have really closed.
                        let response = response?;
                        query_stack.send_to(&response, destination, source).await;
                        Ok::<(), std::io::Error>(())
                    });
                    if let Err(error) = registered {
                        #[cfg(feature = "raw-ip-runtime")]
                        if crate::runtime::raw_ip::tasks::capacity::before_work(&error) {
                            let response = runtime.resolver().busy_response(&buffer[..size]);
                            stack.send_to(&response, destination, source).await;
                            continue;
                        }
                        return Err(std::io::Error::other(error).into());
                    }
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
                                #[cfg(feature = "raw-ip-runtime")]
                                let native = match &owned {
                                    Some(owner) => match owner.native.reserve_reclaimed(
                                        crate::runtime::inbound_operation::raw_ip::native_obligations::ChildKind::Udp,
                                        tokio::time::Instant::now() + Duration::from_secs(5),
                                    ).await {
                                        Ok(native) => Some(native),
                                        Err(error) if crate::runtime::raw_ip::tasks::capacity::before_work(&error) => {
                                            log_pressure_drop(&mut associations, now, source, destination,
                                                AdmissionRejection::ScopeLimit, stack.send_unreachable(&datagram.payload, source, destination));
                                            continue;
                                        }
                                        Err(error) => return Err(std::io::Error::other(error).into()),
                                    },
                                    None => None,
                                };
                                let registered = spawn_association(
                                    &mut tasks,
                                    &mut associations,
                                    AssociationStart {
                                        runtime: runtime.clone(),
                                        stack: Arc::clone(&stack),
                                        inbound_tag: inbound_tag.clone(),
                                        source,
                                        id,
                                        first: datagram,
                                        #[cfg(feature = "raw-ip-runtime")]
                                        native,
                                    },
                                );
                                if let Err(error) = registered {
                                    #[cfg(feature = "raw-ip-runtime")]
                                    if crate::runtime::raw_ip::tasks::capacity::source_admission(&error) {
                                        log_pressure_drop(&mut associations, now, source, destination,
                                            AdmissionRejection::ScopeLimit, stack.send_unreachable(&buffer[..size], source, destination));
                                        continue;
                                    }
                                    return Err(error);
                                }
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
            Some(completed) = tasks.next_completion(), if !tasks.is_empty() => {
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
                        if owned.is_some() {
                            // Scope owns the actual TaskExit; this metadata facade cannot invent one.
                            return Err(std::io::Error::other(error).into());
                        }
                        tracing::warn!(error = %error, "TUN UDP association task panicked");
                    },
                }
            }
            Some(completed) = dns_tasks.next_completion(), if !dns_tasks.is_empty() => {
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
