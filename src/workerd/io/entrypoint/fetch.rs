// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! The fetch event, which the Rust `WorkerEntrypoint` delivers behind the `RUST_ENTRYPOINT_FETCH`
//! autogate.
//!
//! It takes the steps of C++ `WorkerEntrypoint::requestImpl()`, in its order, on the
//! `entrypoint::Fetch` that the C++ `WorkerEntrypoint` made (`src/workerd/io/entrypoint-fetch.h`).
//! The steps do the work, including everything that touches JavaScript; this module decides which
//! steps run.

use std::future::Future;

use cxx::KjException;
use kj_rs::KjOwn;

use crate::ffi::bridge;

/// Starts the fetch event `fetch`: delivers it. Returns the future for the rest of the event,
/// which runs the handler, waits for its deferred proxying and handles their failure. Dropping the
/// future cancels the event; the C++ `Fetch` then drains the request if the handler had not
/// ended.
///
/// # Errors
/// Fails with the exception that delivering the event fails with.
pub fn start(
    mut fetch: KjOwn<bridge::Fetch>,
) -> Result<impl Future<Output = Result<(), KjException>> + 'static, KjException> {
    // Stage 1: set up per-request state.
    bridge::fetch_deliver(fetch.pin_mut())?;
    Ok(finish(fetch))
}

async fn finish(mut fetch: KjOwn<bridge::Fetch>) -> Result<(), KjException> {
    let Err(exception) = run(&mut fetch).await else {
        return Ok(());
    };

    // Stage 4: handle whatever exception escaped the stages above. Exactly one of the branches
    // runs, in this order.
    // TODO(perfetto): the span "WorkerEntrypoint::request() exception" is emitted by
    // prepareFailure(); its flow ends there.
    let exception = bridge::fetch_prepare_failure(fetch.pin_mut(), exception)?;

    if bridge::fetch_is_actor(&fetch) {
        // Tunnel the exception back to the caller.
        let propagated = bridge::fetch_propagate_from_actor(fetch.pin_mut(), exception)?;
        bridge::yield_to_event_loop().await?;
        return Err(propagated);
    }

    if bridge::fetch_has_fail_open_service(&fetch) {
        // Retry the request through the fail-open service.
        return bridge::fetch_fail_open(fetch.pin_mut(), exception).await;
    }

    if bridge::fetch_tunnels_exceptions(&fetch) {
        // Worker-to-worker: tunnel the exception back to the caller.
        let propagated = bridge::fetch_propagate(fetch.pin_mut(), exception)?;
        bridge::yield_to_event_loop().await?;
        return Err(propagated);
    }

    // Otherwise respond with a 5xx.
    bridge::fetch_respond_with_error(fetch.pin_mut(), exception)
}

/// Stages 2 and 3: runs the handler, ends it however it ended, and waits for its deferred
/// proxying.
async fn run(fetch: &mut KjOwn<bridge::Fetch>) -> Result<(), KjException> {
    // Stage 2: run the JavaScript request handler.
    let handled = match bridge::fetch_run_handler(fetch.pin_mut()).await {
        Ok(()) => Ok(()),
        Err(exception) => bridge::fetch_handler_failed(fetch.pin_mut(), exception).await,
    };
    // However the handler ended: drain the request and release the AbortController, before the
    // failure is handled, so that the fail-open service is ready. If the event is cancelled before
    // this, dropping the C++ Fetch does it.
    let ended = bridge::fetch_end_handler(fetch.pin_mut());
    handled?;
    ended?;

    // Stage 3: wait for the deferred-proxy task, if any.
    bridge::fetch_finish_proxying(fetch.pin_mut()).await
}
