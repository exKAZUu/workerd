// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/sqlite.h>

#include <kj/array.h>
#include <kj/common.h>
#include <kj/filesystem.h>
#include <kj/string.h>
#include <kj/time.h>

namespace workerd::server {

// The persisted alarms of a Durable Object namespace: one row per root actor in the `_cf_ALARM`
// table of a SQLite database of their own. The scheduler keeps every alarm in memory as well, and
// reads the table only at startup.
class AlarmStore {
 public:
  struct Alarm {
    kj::String actorId;
    kj::Date scheduledTime;
    // The name the actor was created with via `idFromName()`/`getByName()`, if any.
    kj::Maybe<kj::String> name;
  };

  // Opens the database at `path`, creating it or adding the `actor_name` column that databases
  // older than the column lack.
  AlarmStore(const SqliteDatabase::Vfs& vfs, kj::Path path);

  kj::Array<Alarm> load();

  // Stores `actorId`'s alarm at `scheduledTime`. The stored name changes only when `name` is
  // given: an actor's later alarms are set without it. Returns whether a row changed.
  bool set(kj::StringPtr actorId, kj::Date scheduledTime, kj::Maybe<kj::StringPtr> name);

  // Removes `actorId`'s alarm. Returns whether it had one.
  bool remove(kj::StringPtr actorId);

  // Removes every alarm.
  void clear();

 private:
  kj::Own<SqliteDatabase> db;

  // On upsert we always refresh scheduled_time, but only overwrite actor_name when the incoming
  // row actually carries one. COALESCE keeps the previously-stored name when the incoming one is
  // NULL instead of clobbering it.
  SqliteDatabase::Statement stmtSet = db->prepare(R"(
    INSERT INTO _cf_ALARM VALUES(?, ?, ?)
      ON CONFLICT DO UPDATE SET scheduled_time = excluded.scheduled_time,
        actor_name = COALESCE(excluded.actor_name, _cf_ALARM.actor_name);
  )");
  SqliteDatabase::Statement stmtRemove = db->prepare(R"(
    DELETE FROM _cf_ALARM WHERE actor_id = ?
  )");

  static kj::Own<SqliteDatabase> open(const SqliteDatabase::Vfs& vfs, kj::Path path);
};

}  // namespace workerd::server
