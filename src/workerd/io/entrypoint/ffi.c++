// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "ffi.h"

#include <workerd/io/entrypoint/ffi.rs.h>
#include <workerd/rust/worker/bridge.h>
#include <workerd/util/autogate.h>
#include <workerd/util/use-perfetto-categories.h>

namespace workerd::entrypoint {

kj::Own<Invocation> cxx_entrypoint_take_invocation(CxxEntrypoint& entrypoint) {
  return entrypoint.takeInvocation();
}

IoContext::IncomingRequest& invocation_request(Invocation& invocation) {
  return *invocation.request;
}

kj::Own<IoContext::IncomingRequest> invocation_take_request(Invocation& invocation) {
  return kj::mv(invocation.request);
}

kj::TaskSet& invocation_wait_until_tasks(Invocation& invocation) {
  return invocation.waitUntilTasks;
}

void log_drain_failure(kj::Exception exception) {
  KJ_LOG(ERROR, "draining an entrypoint invocation failed", exception);
}

void trace_scheduled_wait_for_finished() {
  TRACE_EVENT("workerd", "WorkerEntrypoint::runScheduled() waitForFinished()");
}

void invocation_start_scheduled_in_wait_until(
    Invocation& invocation, kj::Date scheduledTime, ::rust::Slice<const kj::byte> cron) {
  startScheduledInWaitUntil(invocation.request->getContext(), scheduledTime,
      kj::str(kj::from<kj_rs::Rust>(cron).asChars()), kj::mv(invocation.entrypointName),
      kj::mv(invocation.versionInfo), kj::mv(invocation.props));
}

void fetch_deliver(Fetch& fetch) {
  fetch.deliver();
}

kj::Promise<void> fetch_run_handler(Fetch& fetch) {
  return fetch.runHandler();
}

kj::Promise<void> fetch_handler_failed(Fetch& fetch, kj::Exception exception) {
  return fetch.handlerFailed(kj::mv(exception));
}

void fetch_end_handler(Fetch& fetch) {
  fetch.endHandler();
}

kj::Promise<void> fetch_finish_proxying(Fetch& fetch) {
  return fetch.finishProxying();
}

kj::Exception fetch_prepare_failure(Fetch& fetch, kj::Exception exception) {
  return fetch.prepareFailure(kj::mv(exception));
}

bool fetch_is_actor(const Fetch& fetch) {
  return fetch.isActor();
}

bool fetch_has_fail_open_service(const Fetch& fetch) {
  return fetch.hasFailOpenService();
}

bool fetch_tunnels_exceptions(const Fetch& fetch) {
  return fetch.tunnelsExceptions();
}

kj::Exception fetch_propagate_from_actor(Fetch& fetch, kj::Exception exception) {
  return fetch.propagateFromActor(kj::mv(exception));
}

kj::Promise<void> fetch_fail_open(Fetch& fetch, kj::Exception exception) {
  return fetch.failOpen(kj::mv(exception));
}

kj::Exception fetch_propagate(Fetch& fetch, kj::Exception exception) {
  return fetch.propagate(kj::mv(exception));
}

void fetch_respond_with_error(Fetch& fetch, kj::Exception exception) {
  fetch.respondWithError(kj::mv(exception));
}

kj::Promise<void> yield_to_event_loop() {
  return kj::yield();
}

namespace {

// The WorkerInterface of a Rust WorkerEntrypoint. The events that Rust delivers go to Rust, and the
// others straight to the C++ WorkerEntrypoint, with no Rust in between.
class RustWorkerEntrypoint final: public WorkerInterface {
 public:
  explicit RustWorkerEntrypoint(::rust::Box<WorkerEntrypoint> entrypoint)
      : entrypoint(kj::mv(entrypoint)),
        cxx(this->entrypoint->cxx()) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    if (!util::Autogate::isEnabled(util::AutogateKey::RUST_ENTRYPOINT_FETCH)) {
      return cxx.request(method, url, headers, requestBody, response);
    }
    // TODO(perfetto): the span "WorkerEntrypoint::request()", with the URL and the flow from
    // `this`, over the whole event; C++ requestImpl() has it.
    // kj::evalNow() makes an exception from the start reject the promise, as it does in C++
    // requestImpl(), a coroutine.
    auto promise = kj::evalNow([&]() {
      return fetch_event_finish(
          fetch_event_start(cxx.takeFetch(method, url, headers, requestBody, response, this)));
    });
    KJ_IF_SOME(canceler, cxx.getCanceler()) {
      return canceler.wrap(kj::mv(promise));
    }
    return promise;
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    return cxx.connect(host, headers, connection, response, settings);
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    return cxx.prewarm(url);
  }

  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    TRACE_EVENT("workerd", "WorkerEntrypoint::runScheduled()");
    // Only the Rust WorkerEntrypoint has this span, so a trace shows which one delivered the event.
    TRACE_EVENT("workerd", "entrypoint::runScheduled()");
    auto scheduled =
        scheduled_event_start(*entrypoint, scheduledTime, cron.asBytes().as<kj_rs::Rust>());
    return scheduled_event_finish(kj::mv(scheduled))
        .then([](::workerd::rust::worker::ScheduledResult result) {
      return kj::from<kj_rs::Rust>(result);
    });
  }

  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    return cxx.runAlarm(scheduledTime, retryCount);
  }

  kj::Promise<kj::Maybe<kj::Date>> abandonAlarm(kj::Date scheduledTime) override {
    return cxx.abandonAlarm(scheduledTime);
  }

  kj::Promise<bool> test() override {
    return cxx.test();
  }

  kj::Promise<void> bench(
      bench::BenchParams::Reader params, bench::BenchReport::Group::Builder report) override {
    return cxx.bench(params, report);
  }

  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return cxx.customEvent(kj::mv(event));
  }

 private:
  ::rust::Box<WorkerEntrypoint> entrypoint;
  // Owned by `entrypoint`.
  CxxEntrypoint& cxx;
};

}  // namespace

kj::Own<WorkerInterface> newWorkerEntrypoint(kj::Own<CxxEntrypoint> cxx) {
  return kj::heap<RustWorkerEntrypoint>(worker_entrypoint_new(kj::mv(cxx)));
}

}  // namespace workerd::entrypoint
