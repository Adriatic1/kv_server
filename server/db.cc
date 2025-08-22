#include "db.hh"
#include "seastar/core/coroutine.hh"

namespace kvdb {

database::~database() {
  for (auto *layer : _layers) {
     delete layer;
  }
}

future<std::string> database::get(std::string key)
{
  assert(!_layers.empty());

  for (auto *layer : _layers) {
     assert(layer != nullptr);
     std::string value = co_await layer->get(key);
     if (!value.empty()) {
        co_return value;
     }
  }
  co_return std::string();
}

future<bool> database::set(std::string key, std::string value)
{
  assert(!_layers.empty());

  for (auto *layer : _layers) {
     assert(layer != nullptr);
     co_await layer->set(key, value);
  }
  co_return true;
}

future<bool> database::del(const std::string key)
{
  assert(!_layers.empty());

  for (auto *layer : _layers) {
     assert(layer != nullptr);
     co_await layer->del(key);
  }
  co_return true;
}

future<std::set<std::string>> database::query(const std::string prefix)
{
  assert(!_layers.empty());

  // only the last layer may store all data (previous ones are caches)
  std::set<std::string> data = co_await _layers.back()->query(prefix);
  co_return data;
}

future<> database::start()
{
  assert(!_layers.empty());

  for (auto *layer : _layers) {
     assert(layer != nullptr);
     // fmt::print("database::start - start layer\n");
     co_await layer->start();
  }
}

future<> database::stop()
{
  assert(!_layers.empty());

  for (auto *layer : _layers) {
     assert(layer != nullptr);
     // fmt::print("database::stop - stop layer\n");
     co_await layer->stop();
  }
}

}; // namespace kvdb
