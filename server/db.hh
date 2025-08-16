#pragma once

#include <string>
#include <vector>

#include <seastar/core/seastar.hh>

using namespace seastar;

namespace kvdb {

/*
  Storage infterface, defines possible storage operations.
*/
class IStorage {
public:
  virtual ~IStorage() = default;
  virtual future<std::string> get(std::string key) = 0;
  virtual future<bool> set(std::string key, std::string value) = 0;
  virtual future<bool> del(std::string key) = 0;
  virtual future<bool> query(std::string prefix, std::vector<std::string> &matches) = 0;
};


/*
  Database implementation, supporting multiple stores
  (in-memory cache + on-disk storage for example), used in order
  as being stored within the container.
  Reading:
   - if key found in 1st store, we skip other stores
   - if key not found in 1st store, we continue with next stores
  Writing:
   - write key/value to each store
  Querying:
   - query only the last store (who must have all keys)
  Database itself acts as a single virtual storage (using the same interface).
*/
class database : public IStorage {
public:
  database(std::vector<IStorage *> stores) : _stores(std::move(stores)) {};
  ~database();

  future<std::string> get(std::string key) override;
  future<bool> set(std::string key, std::string value) override;
  future<bool> del(std::string key) override;
  future<bool> query(std::string prefix, std::vector<std::string> &matches) override;

private:
  std::vector<IStorage *> _stores;
};

}; // namespace kvdb
