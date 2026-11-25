// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "alarm-store.h"

#include <kj/vector.h>

namespace workerd::server {

AlarmStore::AlarmStore(const SqliteDatabase::Vfs& vfs, kj::Path path)
    : db(open(vfs, kj::mv(path))) {}

kj::Own<SqliteDatabase> AlarmStore::open(const SqliteDatabase::Vfs& vfs, kj::Path path) {
  auto db = kj::heap<SqliteDatabase>(vfs, kj::mv(path),
      kj::WriteMode::CREATE | kj::WriteMode::MODIFY | kj::WriteMode::CREATE_PARENT);

  // TODO(sqlite): Do this automatically at a lower layer?
  db->run("PRAGMA journal_mode=WAL;");

  db->run(R"(
    CREATE TABLE IF NOT EXISTS _cf_ALARM (
      actor_id TEXT PRIMARY KEY,
      scheduled_time INTEGER,
      actor_name TEXT
    ) WITHOUT ROWID;
  )");

  // Migrate databases created before the `actor_name` column existed. SQLite has no
  // "ADD COLUMN IF NOT EXISTS", so detect the column via PRAGMA table_info first.
  bool hasNameColumn = false;
  {
    auto info = db->run("PRAGMA table_info(_cf_ALARM);");
    while (!info.isDone()) {
      // Column 1 of table_info output is the column name.
      if (info.getText(1) == "actor_name"_kj) {
        hasNameColumn = true;
        break;
      }
      info.nextRow();
    }
  }
  if (!hasNameColumn) {
    db->run("ALTER TABLE _cf_ALARM ADD COLUMN actor_name TEXT;");
  }

  return db;
}

kj::Array<AlarmStore::Alarm> AlarmStore::load() {
  kj::Vector<Alarm> alarms;
  auto query = db->run(R"(
    SELECT actor_id, scheduled_time, actor_name FROM _cf_ALARM;
  )");
  while (!query.isDone()) {
    alarms.add(Alarm{
      .actorId = kj::str(query.getText(0)),
      .scheduledTime = kj::UNIX_EPOCH + (kj::NANOSECONDS * query.getInt64(1)),
      .name = query.getMaybeText(2).map([](kj::StringPtr name) { return kj::str(name); }),
    });
    query.nextRow();
  }
  return alarms.releaseAsArray();
}

bool AlarmStore::set(kj::StringPtr actorId, kj::Date scheduledTime, kj::Maybe<kj::StringPtr> name) {
  int64_t scheduledTimeNs = (scheduledTime - kj::UNIX_EPOCH) / kj::NANOSECONDS;
  SqliteDatabase::Query::ValuePtr nameParam = nullptr;
  KJ_IF_SOME(n, name) {
    nameParam = n;
  }
  return stmtSet.run(actorId, scheduledTimeNs, nameParam).changeCount() > 0;
}

bool AlarmStore::remove(kj::StringPtr actorId) {
  return stmtRemove.run(actorId).changeCount() > 0;
}

void AlarmStore::clear() {
  db->run("DELETE FROM _cf_ALARM;");
}

}  // namespace workerd::server
