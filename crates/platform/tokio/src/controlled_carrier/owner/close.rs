use super::*;
pub(in crate::controlled_carrier) async fn close_slot(
    slot: &Arc<Slot>,
    deadline: Instant,
) -> Result<CloseReport, BuildError> {
    close_slot_until(slot, deadline, true).await
}
pub(in crate::controlled_carrier) async fn close_native_slot(
    slot: &Arc<Slot>,
    deadline: Instant,
) -> Result<CloseReport, BuildError> {
    close_slot_until(slot, deadline, false).await
}
async fn close_slot_until(
    slot: &Arc<Slot>,
    deadline: Instant,
    wait_users: bool,
) -> Result<CloseReport, BuildError> {
    crate::controlled_carrier::preparation::cancel_staged(slot);
    // The exact Endpoint returns its unique IO and accepted queue worker before
    // ordinary native/LoanOwner closure. Never recurse into adoption.retire
    // from this installed controller or wait the actor from its own body.
    let protocol = { slot.state().protocol.clone() };
    if let Some(protocol) = &protocol {
        if !protocol.wait(deadline).await {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "original protocol cleanup remains retained".into(),
                )],
                ..Default::default()
            });
        }
    }
    // Cleanup is the persistent local receiver even when the original
    // request was cancelled before observing its auxiliary result.
    match timeout_at(
        deadline,
        crate::controlled_carrier::CarrierAdoption { slot: slot.clone() }.accept_auxiliary(),
    )
    .await
    {
        Ok(result) => {
            result?;
        }
        Err(_) => {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(format!(
                    "{} auxiliary join",
                    slot.label()
                ))],
                ..Default::default()
            })
        }
    }
    // Only this original's sealed noncached job is received here. Its real
    // Scope result must be retained before local acceptance can wake the Host.
    match timeout_at(
        deadline,
        crate::controlled_carrier::preparation::accept_job_exit(slot),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(format!(
                    "{} preparation job join",
                    slot.label()
                ))],
                ..Default::default()
            })
        }
    }
    let _closing = match timeout_at(deadline, slot.closing.lock()).await {
        Ok(guard) => guard,
        Err(_) => {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Io(slot.label())],
                ..Default::default()
            })
        }
    };
    // The same original control cannot be closed underneath its negotiated
    // Datagram. Its retained related slot owns the separate native receipt;
    // source ancestry and logical caller settlement remain independent.
    let related = { slot.state().related_child.clone() };
    if let Some(child) = related {
        child.cancel.cancel();
        child.notify();
        let report = Box::pin(close_slot_until(&child, deadline, wait_users)).await?;
        if !report.is_complete() {
            return Ok(report);
        }
        if wait_users {
            if let Some(owner) = slot.owner.upgrade() {
                owner.state().confirmed.append(report);
            }
        }
    }
    loop {
        let changed = slot.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (waiting, driver, control, packet, loan, unknown, closed, auxiliary_pending) = {
            let state = slot.state();
            (
                state.opening
                    || state.spawning
                    || (state.view_live && (!wait_users || state.driver.is_none())),
                state.driver.clone(),
                state.control.clone(),
                state.packet.clone(),
                state.loan.clone(),
                state.unknown.clone(),
                state.closed,
                state.auxiliary.is_some() && !state.auxiliary_accepted,
            )
        };
        if auxiliary_pending {
            // A registration reserved before this close acquired its gate.
            // Local ACK can call arbitrary host wakers, so release the gate
            // before accepting it. Each slot permits only one such registration.
            drop(_closing);
            return Box::pin(close_slot_until(slot, deadline, wait_users)).await;
        }
        if closed && (wait_users || !waiting) {
            if !wait_users {
                // The original native report remains in pending_close for the
                // full settlement. Do not duplicate it or release this slot.
                return Ok(CloseReport::default());
            }
            if !crate::controlled_carrier::users::finish(slot, deadline).await {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(format!(
                        "{} caller/view closure remains unconfirmed",
                        slot.label()
                    ))],
                    ..Default::default()
                });
            }
            // Native/UsersClosed are already independently observable. Only
            // removal/refunding waits for the original caller's actual Host ACK.
            if !crate::controlled_carrier::caller_credit::wait(slot, deadline).await {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(format!(
                        "{} caller registration ACK",
                        slot.label()
                    ))],
                    ..Default::default()
                });
            }
            let fault_received = protocol.as_ref().is_none_or(|phase| phase.fault_received())
                && slot
                    .retirement
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .resource_failure
                    .as_ref()
                    .is_none_or(|fault| fault.ownership_received());
            if !fault_received {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(format!(
                        "{} actual resource fault awaits its retained receiver",
                        slot.label()
                    ))],
                    ..Default::default()
                });
            }
            let report = slot.state().pending_close.take().unwrap_or_default();
            drop(_closing);
            slot.remove();
            slot.notify();
            return Ok(report);
        }
        if waiting {
            if timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Io(slot.label())],
                    ..Default::default()
                });
            }
            continue;
        }
        if let Some(source) = unknown {
            return Err(error(
                slot.instance,
                Stage::Close,
                Reason::Io,
                DeliveryState::Unknown,
            )
            .with_source(source));
        }
        if let Some((id, witness)) = driver {
            let joined = match timeout_at(deadline, witness.wait()).await {
                Ok(joined) => joined,
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Task(id)],
                        ..Default::default()
                    })
                }
            };
            if joined.id() != id {
                return Err(error(
                    slot.instance,
                    Stage::Close,
                    Reason::Internal,
                    DeliveryState::Unknown,
                ));
            }
        }
        let mut report = if let Some(control) = control {
            let mut control = match timeout_at(deadline, control.lock()).await {
                Ok(control) => control,
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(slot.label())],
                        ..Default::default()
                    })
                }
            };
            control.wait_closed(deadline).await.map_err(|mut source| {
                source.component.get_or_insert(slot.instance.component);
                source.impact = crate::contract::Impact::Component;
                source
            })?
        } else if let Some(loan) = loan {
            let mut loan = match timeout_at(deadline, loan.lock()).await {
                Ok(loan) => loan,
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(slot.label())],
                        ..Default::default()
                    })
                }
            };
            let report = loan.close(deadline).await?;
            if report.is_complete() && !loan.closed().is_some_and(|receipt| receipt.matches(&loan))
            {
                return Err(error(
                    slot.instance,
                    Stage::Close,
                    Reason::Internal,
                    DeliveryState::Unknown,
                ));
            }
            report
        } else if let Some(control) = packet {
            match control.try_close() {
                Some(Ok(receipt)) if receipt.matches(&control) => CloseReport {
                    closed_io: vec![slot.label()],
                    ..Default::default()
                },
                Some(Err(source)) => {
                    return Err(error(
                        slot.instance,
                        Stage::Close,
                        Reason::Io,
                        DeliveryState::Unknown,
                    )
                    .with_source(source))
                }
                _ => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(slot.label())],
                        ..Default::default()
                    })
                }
            }
        } else {
            CloseReport::default()
        };
        if report.is_complete() {
            if let Some(protocol) = &protocol {
                report.append(protocol.take_report());
            }
            report.released_resources.push(slot.label());
            {
                let mut state = slot.state();
                // Persist the original native report before waiting for external
                // callers. Cancelled cleanup never loses or fabricates that proof.
                state.closed = true;
                state.pending_close = Some(report);
            }
            slot.confirm_native_closed();
            slot.notify();
            continue;
        }
        return Ok(report);
    }
}
