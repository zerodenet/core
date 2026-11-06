//! A snapshot attempt only after the actual native view and driver returned.
use super::*;
pub(in crate::controlled_carrier) fn try_close_native_slot(
    slot: &Arc<Slot>,
) -> Result<CloseReport, BuildError> {
    let pending = || CloseReport {
        unfinished: vec![Unfinished::Io(slot.label())],
        ..Default::default()
    };
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
    // A logical borrowed control needs its actual logical-return path. Never
    // treat this observation-only attempt as completion of an opaque loan.
    if loan.is_some() {
        return Ok(pending());
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
                .with_source(source))
            }
            _ => return Ok(pending()),
        }
    } else {
        CloseReport::default()
    };
    if report.is_complete() {
        report.released_resources.push(slot.label());
        let mut state = slot.state();
        state.closed = true;
        state.pending_close = Some(report);
        drop(state);
        slot.notify();
        // Keep the actual report for the later full native/users settlement.
        Ok(Default::default())
    } else {
        Ok(report)
    }
}
