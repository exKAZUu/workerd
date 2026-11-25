// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! A Durable Object namespace's alarm scheduler, as the server runs it.
//!
//! Alarms are persisted in the namespace's storage, wait on the factory's timer, and run on
//! actors the namespace starts for them.

use std::rc::Rc;
use std::rc::Weak;
use std::time::Duration;
use std::time::SystemTime;

use futures::future::LocalBoxFuture;
use kj_rs::KjOwn;
use worker::CxxWorkerInterface;

use crate::Result;
use crate::actor::ActorNamespace;
use crate::alarm_scheduler::ActorKey;
use crate::alarm_scheduler::AlarmEnv;
use crate::alarm_scheduler::AlarmStorage;
use crate::alarm_scheduler::StoredAlarm;
use crate::bridge::ffi;
use crate::channels::ActorNamespaceHandle;
use crate::config::Factory;

/// The alarms in a namespace's storage (`AlarmStore`).
pub struct StoredAlarms(pub KjOwn<ffi::ActorStorage>);

impl AlarmStorage for StoredAlarms {
    fn load(&self) -> Result<Vec<StoredAlarm>> {
        Ok(ffi::actor_storage_load_alarms(&self.0)?
            .into_iter()
            .map(|alarm| StoredAlarm {
                actor: ActorKey {
                    id: alarm.actor_id,
                    name: alarm.name.into(),
                },
                scheduled_time: time_from_ns(alarm.scheduled_time_ns),
            })
            .collect())
    }

    fn set(&self, actor: &ActorKey, scheduled_time: SystemTime) -> Result<bool> {
        Ok(ffi::actor_storage_store_alarm(
            &self.0,
            &actor.id,
            actor.name.as_deref().into(),
            time_to_ns(scheduled_time),
        )?)
    }

    fn remove(&self, id: &str) -> Result<bool> {
        Ok(ffi::actor_storage_remove_alarm(&self.0, id)?)
    }

    fn clear(&self) -> Result<()> {
        Ok(ffi::actor_storage_clear_alarms(&self.0)?)
    }
}

/// The event loop, the clock and the actors of a namespace's alarms: the factory's timer and
/// task set, the system clock, and the namespace's actors.
pub struct NamespaceAlarmEnv {
    pub factory: Rc<Factory>,
    pub namespace: Weak<ActorNamespace>,
    pub unique_key: String,
}

impl AlarmEnv for NamespaceAlarmEnv {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn sleep(&self, delay: Duration) -> LocalBoxFuture<'static, ()> {
        let factory = Rc::clone(&self.factory);
        Box::pin(async move { factory.sleep(delay).await })
    }

    fn spawn(&self, task: LocalBoxFuture<'static, ()>) {
        self.factory.spawn_detached(task);
    }

    fn actor(&self, actor: &ActorKey) -> Result<Box<dyn worker::Interface>> {
        let id =
            ffi::actor_id_for_alarm(&self.unique_key, &actor.id, actor.name.as_deref().into())?;
        let namespace = ActorNamespaceHandle(Weak::clone(&self.namespace));
        Ok(Box::new(CxxWorkerInterface::new(
            namespace.actor_for_alarm(id)?,
        )))
    }
}

/// A time `ns` nanoseconds from the Unix epoch, before it for a negative `ns`.
pub fn time_from_ns(ns: i64) -> SystemTime {
    let offset = Duration::from_nanos(ns.unsigned_abs());
    if ns < 0 {
        SystemTime::UNIX_EPOCH - offset
    } else {
        SystemTime::UNIX_EPOCH + offset
    }
}

/// Nanoseconds from the Unix epoch to `time`, negative before it, saturating.
pub fn time_to_ns(time: SystemTime) -> i64 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
        Err(before) => i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |ns| -ns),
    }
}

#[cfg(test)]
#[path = "actor_alarms-test.rs"]
mod tests;
