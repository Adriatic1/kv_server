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

future<bool> CacheShard::set(const std::string key, std::string value)
{
  const auto it = _data.find(key);
  if (it != _data.end()) {
    it->second = value;
  } else {
    _data[key] = value;
  }
  co_return true;
}

future<bool> CacheShard::del(const std::string key)
{
  const auto it = _data.find(key);
  if (it != _data.end()) {
    _data.erase(it);
  }
  co_return true;
}

future<bool> CacheShard::query(const std::string prefix, std::vector<std::string> &matches)
{
  for (auto &[key, val] : _data) {
    if (key.starts_with(prefix)) {
       matches.push_back(key);
    }
  }
  co_return true;
}


CacheStorage::CacheStorage(size_t max_records)
 : _max_records(max_records),
//   _lru(max_records),
   _shards(new seastar::distributed<CacheShard>)
{
  assert(_max_records > 0);
}

CacheStorage::~CacheStorage() {
  assert(_shards == nullptr);
}

future<> CacheStorage::start()
{
   co_await _shards->start();
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
  /* LRU eviction policy
  if (_lru.full()) {
    std::string lru_key = co_await _lru.pop();
  } */

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

future<bool> CacheStorage::query(std::string prefix, std::vector<std::string> &matches)
{
  // TODO: invoke_on_all?, map_reduce?
  co_return true;
}

}; // namespace kvdb
