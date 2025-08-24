#include "store_cache.hh"
#include <cassert>

#include "seastar/core/coroutine.hh"
#include <seastar/core/thread.hh>

namespace kvdb {

future<std::string> CacheShard::get(std::string key)
{
  const auto it = _data.find(key);
  if (it != _data.end()) {
    co_return it->second;
  }
  co_return std::string();
}

future<bool> CacheShard::set(std::string key, std::string value)
{
  const auto it = _data.find(key);
  if (it != _data.end()) {
    it->second = value;
  } else {
    // run LRU eviction policy for this shard
    if (_lru.size() >= _max_records) {
      std::string lru_key = _lru.front();
      fmt::print("CacheShard: LRU evict key {}", lru_key);
      _lru.pop_front();
      co_await del(lru_key);
    }

    _data[key] = value;

    // refresh LRU list (add/move key at the back)
    for (auto it = _lru.begin(); it != _lru.end(); ++it) {
      if (*it == key) {
        _lru.erase(it);
        break;
      }
    }
    _lru.push_back(key);
  }
  co_return true;
}

future<bool> CacheShard::del(const std::string key)
{
  const auto it = _data.find(key);
  if (it != _data.end()) {
    _data.erase(it);

    // refresh LRU list (remove key)
    for (auto it = _lru.begin(); it != _lru.end(); ++it) {
      if (*it == key) {
        _lru.erase(it);
        break;
      }
    }
  }
  co_return true;
}

future<std::set<std::string>> CacheShard::query(const std::string prefix)
{
  // TODO: avoid linear search using lower_bound, will require changing _data from unordered_map to map
  std::set<std::string> res;
  for (auto &[key, val] : _data) {
    if (key.starts_with(prefix)) {
       res.insert(key);
    }
  }
  co_return res;
}


CacheStorage::CacheStorage(size_t max_records)
 : _max_records(max_records),
   _shards(new seastar::distributed<CacheShard>)
{
}

CacheStorage::~CacheStorage() {
  assert(_shards == nullptr);
}

future<> CacheStorage::start()
{
   co_await _shards->start(_max_records);
   co_return;
}

future<> CacheStorage::stop() {
   co_await _shards->stop();
   delete _shards;
   _shards = nullptr;
   co_return;
}

future<std::string> CacheStorage::get(std::string key)
{
  const auto cpu = calc_shard_id(key);
  //fmt::print("CacheStorage::get key:{}\n", key);
  const std::string value = co_await _shards->invoke_on(cpu, &CacheShard::get, key);
  co_return value;
}

future<bool> CacheStorage::set(std::string key, std::string value)
{
  const auto cpu = calc_shard_id(key);
  const bool success = co_await _shards->invoke_on(cpu, &CacheShard::set, key, value);
  co_return success;
}

future<bool> CacheStorage::del(std::string key)
{
  const auto cpu = calc_shard_id(key);
  const bool success = co_await _shards->invoke_on(cpu, &CacheShard::del, key);
  co_return success;
}

static std::set<std::string> set_reducer(std::set<std::string> a, std::set<std::string> b) {
  a.insert(b.begin(), b.end());
  return a;
}

future<std::set<std::string>> CacheStorage::query(std::string prefix)
{
  auto res = co_await _shards->map_reduce0(
         // Mapper: called on each shard instance
         [prefix](CacheShard& shard) { return shard.query(prefix); }, 
         // initial value
         std::set<std::string>(),
         // Reduce function
         set_reducer);
  co_return res;
}

}; // namespace kvdb
