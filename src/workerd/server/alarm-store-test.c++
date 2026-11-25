// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "alarm-store.h"

#include <kj/test.h>

namespace workerd::server {
namespace {

int64_t toNs(kj::Date date) {
  return (date - kj::UNIX_EPOCH) / kj::NANOSECONDS;
}

struct StoreFixture {
  kj::Own<const kj::Directory> dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs{*dir};
  kj::Path path = kj::Path({"alarms"});

  AlarmStore open() {
    return AlarmStore(vfs, path.clone());
  }
};

const kj::Date T0 = kj::UNIX_EPOCH + 1 * kj::HOURS;

KJ_TEST("AlarmStore starts empty") {
  StoreFixture f;
  KJ_EXPECT(f.open().load().size() == 0);
}

KJ_TEST("AlarmStore loads what it stored, across reopening") {
  StoreFixture f;
  {
    auto store = f.open();
    KJ_EXPECT(store.set("named"_kj, T0, "my-name"_kj));
    KJ_EXPECT(store.set("nameless"_kj, T0 + 1 * kj::HOURS, kj::none));
  }

  auto alarms = f.open().load();
  KJ_ASSERT(alarms.size() == 2);
  auto& named = alarms[0].actorId == "named"_kj ? alarms[0] : alarms[1];
  auto& nameless = alarms[0].actorId == "named"_kj ? alarms[1] : alarms[0];
  KJ_EXPECT(named.actorId == "named"_kj);
  KJ_EXPECT(named.scheduledTime == T0);
  KJ_EXPECT(KJ_ASSERT_NONNULL(named.name) == "my-name"_kj);
  KJ_EXPECT(nameless.actorId == "nameless"_kj);
  KJ_EXPECT(nameless.scheduledTime == T0 + 1 * kj::HOURS);
  KJ_EXPECT(nameless.name == kj::none);
}

KJ_TEST("AlarmStore keeps a stored name when an alarm is set again without one") {
  StoreFixture f;
  auto store = f.open();
  KJ_EXPECT(store.set("actor"_kj, T0, "my-name"_kj));
  KJ_EXPECT(store.set("actor"_kj, T0 + 1 * kj::HOURS, kj::none));
  KJ_EXPECT(store.set("actor"_kj, T0 + 2 * kj::HOURS, "new-name"_kj));
  KJ_EXPECT(store.set("actor"_kj, T0 + 3 * kj::HOURS, kj::none));

  auto alarms = store.load();
  KJ_ASSERT(alarms.size() == 1);
  KJ_EXPECT(alarms[0].scheduledTime == T0 + 3 * kj::HOURS);
  KJ_EXPECT(KJ_ASSERT_NONNULL(alarms[0].name) == "new-name"_kj);
}

KJ_TEST("AlarmStore stores times before the epoch and to the nanosecond") {
  StoreFixture f;
  auto store = f.open();
  auto early = kj::UNIX_EPOCH - 1 * kj::HOURS + 1 * kj::NANOSECONDS;
  store.set("actor"_kj, early, kj::none);
  auto alarms = store.load();
  KJ_ASSERT(alarms.size() == 1);
  KJ_EXPECT(alarms[0].scheduledTime == early);
}

KJ_TEST("AlarmStore reports whether remove() removed an alarm") {
  StoreFixture f;
  auto store = f.open();
  KJ_EXPECT(!store.remove("actor"_kj));
  store.set("actor"_kj, T0, kj::none);
  KJ_EXPECT(store.remove("actor"_kj));
  KJ_EXPECT(!store.remove("actor"_kj));
  KJ_EXPECT(store.load().size() == 0);
}

KJ_TEST("AlarmStore clear() removes every alarm") {
  StoreFixture f;
  {
    auto store = f.open();
    store.set("a"_kj, T0, kj::none);
    store.set("b"_kj, T0, "name"_kj);
    store.clear();
    KJ_EXPECT(store.load().size() == 0);
  }
  KJ_EXPECT(f.open().load().size() == 0);
}

KJ_TEST("AlarmStore migrates a database created before the actor_name column existed") {
  StoreFixture f;

  // The schema of a database persisted by an older workerd, which lacks `actor_name`, with one
  // pending alarm.
  {
    SqliteDatabase db(f.vfs, f.path.clone(), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
    db.run("PRAGMA journal_mode=WAL;");
    db.run(R"(
      CREATE TABLE _cf_ALARM (
        actor_id TEXT PRIMARY KEY,
        scheduled_time INTEGER
      ) WITHOUT ROWID;
    )");
    db.run("INSERT INTO _cf_ALARM VALUES (?, ?)", "old-actor"_kj, toNs(T0));
  }

  {
    auto store = f.open();
    auto alarms = store.load();
    KJ_ASSERT(alarms.size() == 1);
    KJ_EXPECT(alarms[0].actorId == "old-actor"_kj);
    KJ_EXPECT(alarms[0].scheduledTime == T0);
    KJ_EXPECT(alarms[0].name == kj::none);
    // The migrated table takes names.
    store.set("old-actor"_kj, T0, "my-name"_kj);
  }

  {
    SqliteDatabase db(f.vfs, f.path.clone(), kj::WriteMode::MODIFY);
    auto query = db.run("SELECT actor_name FROM _cf_ALARM WHERE actor_id = ?", "old-actor"_kj);
    KJ_ASSERT(!query.isDone());
    KJ_EXPECT(KJ_ASSERT_NONNULL(query.getMaybeText(0)) == "my-name"_kj);
  }

  // Opening a migrated database again leaves it as it is.
  auto alarms = f.open().load();
  KJ_ASSERT(alarms.size() == 1);
  KJ_EXPECT(KJ_ASSERT_NONNULL(alarms[0].name) == "my-name"_kj);
}

}  // namespace
}  // namespace workerd::server
