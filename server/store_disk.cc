#include "store_disk.hh"
#include <cassert>

#include "seastar/core/coroutine.hh"
#include <seastar/core/file-types.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/fstream.hh>
#include <string_view>

namespace kvdb {

// On-disk data is stored in separate file for each CPU core shard.
// Data consists of individual key/value records stored sequentially,
// records are always appended to the existing file.
// Records being deleted are kept in place, just marking their status
// flag as deleted.
//
// Record layout:
// - 1 byte record status: 2-valid, 1-deleted
// - 2 byte key length (unsigned)
// - 8 bytes value length (unsigned)
// - key data bytes follow
// - value data bytes follow

constexpr size_t HEADER_SIZE = 11;  // first 3 members of the above record
constexpr unsigned char REC_VALID = 2;
constexpr unsigned char REC_DELETED = 1;

std::string get_file_name() {
  return fmt::format("kvdb_data.{:0>3}.bin", this_shard_id());
}

future<> DiskShard::build_db_index() {
  // read file sequentially and build the in-memory index of value offsets
  _index.clear();
  uint64_t pos = 0;
  uint64_t fsize = co_await _f.size();
  fmt::print("DiskShard {:0>3}: build index - fsize:{}\n", this_shard_id(), fsize);

  while (pos < fsize) {
     // read record header only
     auto stream = make_file_input_stream(_f, pos, HEADER_SIZE);
     temporary_buffer<char> data = co_await stream.read();
     const char *record = data.get();
     const unsigned char rec_status = *(record);
     const uint16_t key_size = *(uint16_t *)(record + 1);
     const uint64_t val_size = *(uint64_t *)(record + 3);

     if (rec_status != REC_VALID) {
        if (rec_status == REC_DELETED) {
        fmt::print("DiskShard {:0>3}: build index - got deleted entry at {}\n", this_shard_id(), pos);
        pos += HEADER_SIZE + key_size + val_size;
        continue;
       }
       fmt::print("DiskShard {:0>3}: build index - got invalid entry at {}\n", this_shard_id(), pos);
       break;
     }

     // valid record, read the key name
     auto stream1 = make_file_input_stream(_f, pos + HEADER_SIZE, (uint64_t)key_size);
     temporary_buffer<char> name = co_await stream1.read();
     std::string_view key(name.get(), name.size());
     fmt::print("DiskShard {:0>3}: build index - got entry:{}, size {} at {}\n", this_shard_id(), key, val_size, pos + HEADER_SIZE + key_size);

     _index[std::string(key)] = std::make_pair<>(pos + HEADER_SIZE + key_size, val_size);
     pos += HEADER_SIZE + key_size + val_size;
  }
  _end_offset = pos;
}

future<> DiskShard::start() {
    //fmt::print("DiskShard {:0>3}: start\n", this_shard_id());
    std::string name = get_file_name();
    //fmt::print("DiskShard {:0>3}: open file - {}\n", this_shard_id(), name);
    _f = co_await open_file_dma(name, open_flags::rw|open_flags::create|open_flags::dsync);
    co_await build_db_index();
    co_return;
}

future<> DiskShard::stop() {
    fmt::print("DiskShard {:0>3}: close file\n", this_shard_id());
    // as we use DMA to write entire blocks, we need to truncate 
    // excess data on end
    uint64_t fsize = co_await _f.size();
    if (fsize > _end_offset) {
      fmt::print("DiskShard {:0>3}: truncate file from {} to {}\n", this_shard_id(), fsize, _end_offset);
      co_await _f.truncate(_end_offset);
    }
    co_await _f.close();
    co_return;
}

future<std::string> DiskShard::get(std::string key)
{
  const auto it = _index.find(key);
  if (it != _index.end()) {
    // key found in index, now read actual data in file
    auto stream = make_file_input_stream(_f, it->second.first, it->second.second);
    temporary_buffer<char> value = co_await stream.read();
    co_return std::string(value.get(), value.size());
  }
  co_return std::string();
}

future<bool> DiskShard::set(std::string key, std::string value)
{
  //fmt::print("DiskShard {:0>3}: set [{},{}]\n", this_shard_id(), key, value);
  const auto it = _index.find(key);
  if (it != _index.end()) {
    //fmt::print("DiskShard {:0>3}: set [{},{}] call del\n", this_shard_id(), key, value);
    co_await del(key);  // mark old record as deleted
  }

  // append new record
  //fmt::print("DiskShard {:0>3}: set [{},{}] get file size\n", this_shard_id(), key, value);
  uint64_t pos = _end_offset;
  const uint16_t key_size = key.size();
  const uint64_t val_size = value.size();

  //fmt::print("DiskShard {:0>3}: set [{},{}] prepare header\n", this_shard_id(), key, value);
  const auto alignment = _f.disk_write_dma_alignment();
  uint64_t aligned_pos = align_down<uint64_t>(pos, alignment);
  const auto rec_size = HEADER_SIZE + key_size + val_size;
  uint64_t aligned_size = align_up<uint64_t>(rec_size, alignment);
  //fmt::print("DiskShard {:0>3}: set [{},{}] write alignment {}, record size {}\n", this_shard_id(), key, value, alignment, rec_size);

  std::unique_ptr<char[], seastar::free_deleter> buf =
     seastar::allocate_aligned_buffer<char>(aligned_size, alignment);

  // read, modify, write cycle
  co_await _f.dma_read(aligned_pos, buf.get(), alignment);

  uint64_t offset = pos - aligned_pos;
  memset(buf.get() + offset, REC_VALID, 1);  // 1st byte - valid record
  memcpy(buf.get() + offset + 1, &key_size, sizeof(uint16_t));
  memcpy(buf.get() + offset + 3, &val_size, sizeof(uint64_t));
  memcpy(buf.get() + offset + 3 + sizeof(uint64_t), key.c_str(), key_size);
  memcpy(buf.get() + offset + 3 + sizeof(uint64_t) + key_size, value.c_str(), val_size);

  //fmt::print("DiskShard {:0>3}: set [{},{}] write data (pos={}, len={})\n", this_shard_id(), key, value, pos, rec_size);
  // TODO: error handling, finish partial writes using the loop
  co_await _f.dma_write(aligned_pos, buf.get(), aligned_size);
  //fmt::print("DiskShard {:0>3}: set [{},{}] flush\n", this_shard_id(), key, value);
  co_await _f.flush();

  // update index
  _index[key] = std::make_pair<>(pos + HEADER_SIZE + key_size, val_size);
  _end_offset = pos + rec_size;

  //fmt::print("DiskShard {:0>3}: set done [{},{}]\n", this_shard_id(), key, value);
  co_return true;
}

future<bool> DiskShard::del(const std::string key)
{
  //fmt::print("DiskShard {:0>3}: del [{}]\n", this_shard_id(), key);

  const auto it = _index.find(key);
  if (it != _index.end()) {
    uint64_t val_pos = it->second.first;
    uint64_t pos = val_pos - HEADER_SIZE - key.size();
    const auto alignment = _f.disk_write_dma_alignment();
    uint64_t aligned_pos = align_down(pos, alignment);

    // write tombstone mark
    std::unique_ptr<char[], seastar::free_deleter> buf =
        seastar::allocate_aligned_buffer<char>(alignment, alignment);

    // read, modify, write cycle
    co_await _f.dma_read(aligned_pos, buf.get(), alignment);

    uint64_t offset = pos - aligned_pos;
    memset(buf.get() + offset, REC_DELETED, 1);  // 1st byte - invalid record

    //fmt::print("DiskShard {:0>3}: del [{}] write data (pos={})\n", this_shard_id(), key, pos);
    co_await _f.dma_write(aligned_pos, buf.get(), alignment);
    co_await _f.flush();

    // update index
    _index.erase(it);
  }
  co_return true;
}

future<std::set<std::string>> DiskShard::query(const std::string prefix)
{
  // only use in-memory index for this operation, no need to touch the disk
  // TODO: avoid linear search using lower_bound, will require changing _data from unordered_map to map
  std::set<std::string> res;
  for (auto &[key, val] : _index) {
    if (key.starts_with(prefix)) {
       res.insert(key);
    }
  }
  co_return res;
}


DiskStorage::DiskStorage()
 : _shards(new seastar::distributed<DiskShard>)
{
}

DiskStorage::~DiskStorage() {
  assert(_shards == nullptr);
}

future<> DiskStorage::start()
{
   //fmt::print("DiskStorage: start\n");
   co_await _shards->start();
   co_await _shards->invoke_on_all([] (DiskShard &shard) {return shard.start();});
   //fmt::print("DiskStorage: start done\n");
   co_return;
}

future<> DiskStorage::stop() {
   //fmt::print("DiskStorage: stop\n");
   co_await _shards->stop();
   delete _shards;
   _shards = nullptr;
   co_return;
}

future<std::string> DiskStorage::get(std::string key)
{
  const auto cpu = calc_shard_id(key);
  //fmt::print("DiskStorage::get key:{}\n", key);
  const std::string value = co_await _shards->invoke_on(cpu, &DiskShard::get, key);
  co_return value;
}

future<bool> DiskStorage::set(std::string key, std::string value)
{
  const auto cpu = calc_shard_id(key);
  //fmt::print("DiskStorage: set on cpu{} [{},{}]\n", cpu, key, value);
  const bool success = co_await _shards->invoke_on(cpu, &DiskShard::set, key, value);
  co_return success;
}

future<bool> DiskStorage::del(std::string key)
{
  const auto cpu = calc_shard_id(key);
  //fmt::print("DiskStorage: del on cpu{} [{}]\n", cpu, key);
  const bool success = co_await _shards->invoke_on(cpu, &DiskShard::del, key);
  co_return success;
}

// calculate union of two sets
static std::set<std::string> set_reducer(std::set<std::string> a, std::set<std::string> b) {
  a.insert(b.begin(), b.end());
  return a;
}

future<std::set<std::string>> DiskStorage::query(std::string prefix)
{
  auto res = co_await _shards->map_reduce0(
         // Mapper: called on each shard instance
         [prefix](DiskShard& shard) { return shard.query(prefix); }, 
         // initial value
         std::set<std::string>(),
         // Reduce function
         set_reducer);
  co_return res;
}

}; // namespace kvdb
