#pragma once

#include <string>
#include <set>
#include <list>
#include "db.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/distributed.hh>

using namespace seastar;

namespace kvdb {

class CacheShard {
public:
  CacheShard(size_t max_records) : _max_records(max_records) {}

  future<std::string> get(std::string key);
  future<bool> set(std::string key, std::string value);
  future<bool> del(std::string key);
  future<std::set<std::string>> query(std::string prefix);

  future<> stop() {
      return make_ready_future();
  }

protected:
  std::unordered_map<std::string, std::string> _data;
  // max records per shard is easier to implement
  // no shared queue contention
  size_t _max_records;
  std::list<std::string> _lru;
};

/*
  Implement in-memory cache with limited number of records,
  using LRU eviction policy.
*/
class CacheStorage : public IStorage {
public:
  CacheStorage(size_t max_records);
  virtual ~CacheStorage();

  future<> start() override;
  future<> stop() override;

  future<std::string> get(std::string key) override;
  future<bool> set(std::string key, std::string value) override;
  future<bool> del(std::string key) override;
  future<std::set<std::string>> query(std::string prefix) override;

private:
  unsigned int calc_shard_id(std::string &key) const { return std::hash<std::string>{}(key) % smp::count; }

  size_t _max_records;
  // data sharded to a number of cores
  seastar::distributed<CacheShard> *_shards;
};

}; // namespace kvdb
