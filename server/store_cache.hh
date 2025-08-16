#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "db.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/distributed.hh>

using namespace seastar;

namespace kvdb {

class CacheShard {
public:

  future<std::string> get(std::string key);
  future<bool> set(std::string key, std::string value);
  future<bool> del(std::string key);
  future<bool> query(std::string prefix, std::vector<std::string> &matches);

  future<> stop() {
      return make_ready_future();
  }

protected:
  std::unordered_map<std::string, std::string> _data;
};

/*
  Implement in-memory cache with limited number of records,
  using LRU eviction policy.
*/
class CacheStorage : public IStorage {
public:
  CacheStorage(size_t max_records);
  virtual ~CacheStorage();

  future<> start();
  future<> stop();

  future<std::string> get(std::string key) override;
  future<bool> set(std::string key, std::string value) override;
  future<bool> del(std::string key) override;
  future<bool> query(std::string prefix, std::vector<std::string> &matches) override;

private:
  unsigned int calc_shard_id(std::string &key) const { return std::hash<std::string>{}(key) % smp::count; }

  size_t _max_records;
  // seastar::queue<std::string> _lru;  // LRU tracking

  // data sharded to a number of cores
  seastar::distributed<CacheShard> *_shards;
};

}; // namespace kvdb
