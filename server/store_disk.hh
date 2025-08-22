#pragma once

#include <string>
#include <set>
#include <unordered_map>
#include "db.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/file.hh>

using namespace seastar;

namespace kvdb {

class DiskShard {
public:

  future<std::string> get(std::string key);
  future<bool> set(std::string key, std::string value);
  future<bool> del(std::string key);
  future<std::set<std::string>> query(std::string prefix);

  future<> start();
  future<> stop();

protected:
  future<> build_db_index();

protected:
  file _f;
  // [offset, size] for disk record "value" member from key
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> _index;
  uint64_t _end_offset{0};
};

/*
  Implement on-disk database.
*/
class DiskStorage : public IStorage {
public:
  DiskStorage();
  virtual ~DiskStorage();

  future<> start() override;
  future<> stop() override;

  future<std::string> get(std::string key) override;
  future<bool> set(std::string key, std::string value) override;
  future<bool> del(std::string key) override;
  future<std::set<std::string>> query(std::string prefix) override;

private:
  unsigned int calc_shard_id(std::string &key) const { return std::hash<std::string>{}(key) % smp::count; }

  // data sharded to a number of cores
  seastar::distributed<DiskShard> *_shards;
};

}; // namespace kvdb
