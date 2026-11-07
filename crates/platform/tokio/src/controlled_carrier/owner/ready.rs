//! A snapshot attempt only after the actual native view and driver returned.
use super::*;
pub(in crate::controlled_carrier) fn try_close_native_slot(
    slot: &Arc<Slot>,
) -> Result<CloseReport, BuildError> {
    let pending = || CloseReport {
        unfinished: vec![Unfinished::Io(slot.label())],
        ..Default::default()
    };
    // Local failure acceptance invokes Host wakers: receive the exact job
    // outside this original's close gate and all Slot/Owner state locks.
    crate::controlled_carrier::preparation::try_accept_job_exit(slot)?;
    let protocol = { slot.state().protocol.clone() };
    if protocol
        .as_ref()
        .is_some_and(|protocol| !protocol.complete())
    {
        return Ok(pending());
    }
    let Ok(_closing) = slot.closing.try_lock() else {
        return Ok(pending());
    };
    let (blocked, driver, control, packet, loan, unknown, closed, related) = {
        let state = slot.state();
        (
            state.opening
                || state.spawning
                || state.view_live
                || (state.auxiliary.is_some() && !state.auxiliary_accepted),
            state.driver.clone(),
            state.control.clone(),
            state.packet.clone(),
            state.loan.clone(),
            state.unknown.clone(),
            state.closed,
            state.related_child.clone(),
        )
    };
    if blocked {
        return Ok(pending());
    }
    if closed {
        return Ok(Default::default());
    }
    if let Some((id, driver)) = driver {
        let Some(joined) = driver.try_joined() else {
            return Ok(pending());
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
    if let Some(source) = unknown {
        return Err(error(
            slot.instance,
            Stage::Close,
            Reason::Io,
            DeliveryState::Unknown,
        )
        .with_source(source));
    }
    if let Some(related) = related {
        let report = try_close_native_slot(&related)?;
        if !report.is_complete() {
            return Ok(report);
        }
        // Full related logical settlement is still required before the TCP
        // parent's ordinary retirement. This attempt grants no UsersClosed.
        if !related.state().closed {
            return Ok(pending());
        }
    }
    let mut report = if let Some(control) = control {
        let Ok(mut control) = control.try_lock() else {
            return Ok(pending());
        };
        control.try_close_once().map_err(|mut source| {
            source.component.get_or_insert(slot.instance.component);
            source.impact = crate::contract::Impact::Component;
            source
        })?
    } else if let Some(loan) = loan {
        let Ok(mut loan) = loan.try_lock() else {
            return Ok(pending());
        };
        let report = loan.try_close()?;
        if report.is_complete() && !loan.closed().is_some_and(|closed| closed.matches(&loan)) {
            return Err(error(
                slot.instance,
                Stage::Close,
                Reason::Internal,
                DeliveryState::Unknown,
            ));
        }
        report
    } else if let Some(packet) = packet {
        match packet.try_close() {
            Some(Ok(closed)) if closed.matches(&packet) => CloseReport {
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
                .with_source(source));
            }
            _ => return Ok(pending()),
        }
    } else {
        CloseReport::default()
    };
    if report.is_complete() {
        if let Some(protocol) = &protocol {
            report.append(protocol.take_report());
        }
        report.released_resources.push(slot.label());
        let mut state = slot.state();
        state.closed = true;
        state.pending_close = Some(report);
        drop(state);
        slot.confirm_native_closed();
        slot.notify();
        // Keep the actual report for the later full native/users settlement.
        Ok(Default::default())
    } else {
        Ok(report)
    }
}

/// Full nonwaiting settlement still requires the same native receipt, returned
/// users and actual caller join. Native-only completion cannot refund this slot.
pub(in crate::controlled_carrier) fn try_close_slot(
    slot: &Arc<Slot>,
) -> Result<CloseReport, BuildError> {
    crate::controlled_carrier::preparation::cancel_staged(slot);
    let related = { slot.state().related_child.clone() };
    if let Some(child) = related {
        child.cancel.cancel();
        child.notify();
        let report = try_close_slot(&child)?;
        if !report.is_complete() {
            return Ok(report);
        }
        if let Some(owner) = slot.owner.upgrade() {
            owner.state().confirmed.append(report);
        }
    }
    let report = try_close_native_slot(slot)?;
    if !report.is_complete() {
        return Ok(report);
    }
    let pending = || CloseReport {
        unfinished: vec![Unfinished::Resource(format!(
            "{} caller/view closure remains unconfirmed",
            slot.label()
        ))],
        ..Default::default()
    };
    let Ok(_closing) = slot.closing.try_lock() else {
        return Ok(pending());
    };
    let caller = {
        let state = slot.state();
        if !state.closed || state.users.as_ref().is_some_and(|users| !users.ready()) {
            return Ok(pending());
        }
        state.users.as_ref().and_then(|users| users.caller_joined())
    };
    if let Some(joined) = caller.filter(|joined| joined.result().is_err()) {
        if let Some(owner) = slot.owner.upgrade() {
            owner
                .completion
                .caller_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(joined);
        }
    }
    if !crate::controlled_carrier::caller_credit::released(slot) {
        return Ok(CloseReport {
            unfinished: vec![Unfinished::Resource(format!(
                "{} caller registration ACK",
                slot.label()
            ))],
            ..Default::default()
        });
    }
    let retained = slot.state().pending_close.take();
    let mut report = retained.unwrap_or_default();
    super::task_report::append(slot, &mut report);
    drop(_closing);
    slot.remove();
    slot.notify();
    Ok(report)
}
