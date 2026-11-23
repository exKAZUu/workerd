// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/entrypoint-fetch.h>
#include <workerd/io/entrypoint-invocation.h>
#include <workerd/io/worker-interface.h>

#include <rust/cxx.h>

#include <kj/async.h>
#include <kj/time.h>

namespace workerd::rust::worker {
struct ScheduledResult;
}  // namespace workerd::rust::worker

// The calls through which Rust delivers an event of a WorkerEntrypoint (see `ffi.rs`). Each one
// does its work when it is called; an async one returns a promise for what remains to wait for.
namespace workerd::entrypoint {

// CxxEntrypoint::takeInvocation().
kj::Own<Invocation> cxx_entrypoint_take_invocation(CxxEntrypoint& entrypoint);

// The event's request, which Rust passes to the IoContext::IncomingRequest calls of the io-context
// crate. Taken out of the invocation, by invocation_take_request(), to be drained or finished.
IoContext::IncomingRequest& invocation_request(Invocation& invocation);
kj::Own<IoContext::IncomingRequest> invocation_take_request(Invocation& invocation);

// The waitUntil tasks of the WorkerEntrypoint, which outlive the invocation: a request drains
// into them.
kj::TaskSet& invocation_wait_until_tasks(Invocation& invocation);

// startScheduledInWaitUntil() (`io/entrypoint-invocation.h`), which
// WorkerEntrypoint::runScheduled() also calls, for the event's request, for `scheduledTime` and
// the cron pattern `cron`. Takes the event's entrypoint name, version info and props.
void invocation_start_scheduled_in_wait_until(
    Invocation& invocation, kj::Date scheduledTime, ::rust::Slice<const kj::byte> cron);

// Logs the exception that draining a request failed with. Rust drains a request when it drops
// the event, so it can't throw it.
void log_drain_failure(kj::Exception exception);

// The span "WorkerEntrypoint::runScheduled() waitForFinished()", which C++ emits as it starts
// waiting for a scheduled event's request to finish.
void trace_scheduled_wait_for_finished();

// The steps of a fetch event, for `fetch.rs`: the methods of the same names of entrypoint::Fetch
// (`io/entrypoint-fetch.h`), which WorkerEntrypoint::request() takes in C++.
void fetch_deliver(Fetch& fetch);
kj::Promise<void> fetch_run_handler(Fetch& fetch);
kj::Promise<void> fetch_handler_failed(Fetch& fetch, kj::Exception exception);
void fetch_end_handler(Fetch& fetch);
kj::Promise<void> fetch_finish_proxying(Fetch& fetch);
kj::Exception fetch_prepare_failure(Fetch& fetch, kj::Exception exception);
bool fetch_is_actor(const Fetch& fetch);
bool fetch_has_fail_open_service(const Fetch& fetch);
bool fetch_tunnels_exceptions(const Fetch& fetch);
kj::Exception fetch_propagate_from_actor(Fetch& fetch, kj::Exception exception);
kj::Promise<void> fetch_fail_open(Fetch& fetch, kj::Exception exception);
kj::Exception fetch_propagate(Fetch& fetch, kj::Exception exception);
void fetch_respond_with_error(Fetch& fetch, kj::Exception exception);

// kj::yield(): lets a pending cancellation take effect before the event fails.
kj::Promise<void> yield_to_event_loop();

// Makes a Rust WorkerEntrypoint (see `lib.rs`) that owns `cxx`, and returns its WorkerInterface.
// It delivers the scheduled event from Rust and passes every other event to `cxx`, which then
// does its work as it would on its own.
kj::Own<WorkerInterface> newWorkerEntrypoint(kj::Own<CxxEntrypoint> cxx);

}  // namespace workerd::entrypoint
