#include "db.hh"
#include "seastar/core/coroutine.hh"

namespace kvdb {

database::~database() {
  for (auto *store : _stores) {
     delete store;
  }
}

future<std::string> database::get(std::string key)
{
  assert(!_stores.empty());

  for (auto *store : _stores) {
     assert(store != nullptr);
     std::string value = co_await store->get(key);
     if (!value.empty()) {
        co_return value;
     }
  }
  co_return std::string();
}

future<bool> database::set(std::string key, std::string value)
{
  assert(!_stores.empty());

  for (auto *store : _stores) {
     assert(store != nullptr);
     co_await store->set(key, value);
  }
  co_return true;
}

future<bool> database::del(const std::string key)
{
  assert(!_stores.empty());

  for (auto *store : _stores) {
     assert(store != nullptr);
     co_await store->del(key);
  }
  co_return true;
}

future<std::set<std::string>> database::query(const std::string prefix)
{
  assert(!_stores.empty());

  // only the last layer may store all data (previous ones are caches)
  std::set<std::string> data = co_await _stores.back()->query(prefix);
  co_return data;
}

}; // namespace kvdb
