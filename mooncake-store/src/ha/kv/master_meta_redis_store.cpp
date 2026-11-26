#include "ha/kv/master_meta_redis_store.h"


#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <unordered_map>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#include <ylt/struct_pack.hpp>
#endif

namespace mooncake {

MasterMetaRedisStore::MasterMetaRedisStore(Config config)
    : config_(std::move(config)) {
    if (config_.num_buckets == 0) config_.num_buckets = 1024;
    // Stripe writes across >=1 connections. A value <=1 keeps the legacy single
    // connection; a larger pool lets a sharded Redis handle writes to different
    // buckets in parallel. conns_ is never empty, so slot 0 always exists for
    // segment writes and sequential recovery.
    const size_t n =
        config_.write_conns > 0 ? static_cast<size_t>(config_.write_conns) : 1;
    conns_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        conns_.push_back(std::make_unique<ConnSlot>());
    }
#ifdef STORE_USE_REDIS
    if (Enabled()) {
        for (auto& slot : conns_) {
            slot->worker = std::thread([this, slot = slot.get()] {
                ActorLoop(*slot);
            });
        }
    }
#endif
}

MasterMetaRedisStore::~MasterMetaRedisStore() {
#ifdef STORE_USE_REDIS
    for (auto& slot : conns_) {
        {
            std::lock_guard<std::mutex> lock(slot->mutex);
            slot->stopping = true;
        }
        slot->work_cv.notify_all();
        slot->space_cv.notify_all();
    }
    for (auto& slot : conns_) {
        if (slot->worker.joinable()) slot->worker.join();
    }
#endif
}

#ifdef STORE_USE_REDIS
bool MasterMetaRedisStore::Submit(ConnSlot& slot,
                                  std::function<bool(ConnSlot&)> job) {
    std::unique_lock<std::mutex> lock(slot.mutex);
    slot.space_cv.wait(lock, [&slot] {
        return slot.stopping || slot.queue.size() < ConnSlot::kQueueLimit;
    });
    if (slot.stopping) return false;
    slot.queue.push_back(Job{std::move(job), std::promise<bool>()});
    auto result = slot.queue.back().done.get_future();
    slot.work_cv.notify_one();
    lock.unlock();
    return result.get();
}

void MasterMetaRedisStore::ActorLoop(ConnSlot& slot) {
    while (true) {
        Job job;
        {
            std::unique_lock<std::mutex> lock(slot.mutex);
            slot.work_cv.wait(lock, [&slot] {
                return slot.stopping || !slot.queue.empty();
            });
            if (slot.queue.empty()) {
                if (slot.stopping) break;
                continue;
            }
            job = std::move(slot.queue.front());
            slot.queue.pop_front();
            slot.space_cv.notify_one();
        }
        bool ok = false;
        try {
            if (job.run) ok = job.run(slot);
        } catch (const std::exception& e) {
            LOG(ERROR) << "MetaRedis actor job threw: " << e.what();
        } catch (...) {
            LOG(ERROR) << "MetaRedis actor job threw an unknown exception";
        }
        job.done.set_value(ok);
    }
}
#endif

// ---- key/field codec -------------------------------------------------------

std::string MasterMetaRedisStore::EncodeField(std::string_view tenant_id,
                                              std::string_view key) {
    // Fixed 4-hex-digit tenant length prefix keeps the field binary-safe and
    // collision-free for arbitrary key bytes. Tenant ids are short (<= 65535).
    std::string out;
    out.reserve(4 + tenant_id.size() + key.size());
    char buf[5];
    std::snprintf(buf, sizeof(buf), "%04zx",
                  std::min<size_t>(tenant_id.size(), 0xFFFF));
    out.append(buf, 4);
    out.append(tenant_id.data(), tenant_id.size());
    out.append(key.data(), key.size());
    return out;
}

bool MasterMetaRedisStore::DecodeField(std::string_view field,
                                       std::string& tenant_id,
                                       std::string& key) {
    if (field.size() < 4) return false;
    unsigned len = 0;
    for (int i = 0; i < 4; ++i) {
        const char c = field[i];
        unsigned d;
        if (c >= '0' && c <= '9')
            d = static_cast<unsigned>(c - '0');
        else if (c >= 'a' && c <= 'f')
            d = static_cast<unsigned>(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F')
            d = static_cast<unsigned>(c - 'A' + 10);
        else
            return false;
        len = len * 16 + d;
    }
    if (field.size() < 4 + len) return false;
    tenant_id.assign(field.data() + 4, len);
    key.assign(field.data() + 4 + len, field.size() - 4 - len);
    return true;
}

// ---- key builders ----------------------------------------------------------

std::string MasterMetaRedisStore::FormatBucket(size_t bucket,
                                               size_t num_buckets) {
    const size_t max_idx = num_buckets == 0 ? 0 : num_buckets - 1;
    const int width =
        std::max<int>(2, static_cast<int>(std::to_string(max_idx).size()));
    std::ostringstream oss;
    oss << std::setw(width) << std::setfill('0') << bucket;
    return oss.str();
}

std::string MasterMetaRedisStore::ObjHashPrefix() const {
    const std::string prefix =
        config_.ns.empty() ? std::string() : config_.ns + ":";
    return prefix + "obj:";
}

std::string MasterMetaRedisStore::ObjHashKey(size_t bucket) const {
    return ObjHashPrefix() + FormatBucket(bucket, config_.num_buckets);
}

std::string MasterMetaRedisStore::SegHashKey() const {
    const std::string prefix =
        config_.ns.empty() ? std::string() : config_.ns + ":";
    return prefix + "seg";
}

int MasterMetaRedisStore::ParseBucketFromHashKey(
    std::string_view hash_key) const {
    const std::string prefix = ObjHashPrefix();
    if (hash_key.size() <= prefix.size()) return -1;
    if (hash_key.substr(0, prefix.size()) != prefix) return -1;
    const std::string suffix(hash_key.substr(prefix.size()));
    if (suffix.empty() ||
        suffix.find_first_not_of("0123456789") != std::string::npos) {
        return -1;
    }
    int idx = -1;
    try {
        idx = std::stoi(suffix);
    } catch (const std::exception&) {
        return -1;
    }
    if (idx < 0 || static_cast<size_t>(idx) >= config_.num_buckets) return -1;
    return idx;
}

// ---- write side ------------------------------------------------------------
// Object writes (PutObject/DelObject) do the HSET/HDEL and return success/
// failure (policy is the caller's); segment writes are fire-and-forget.

#ifdef STORE_USE_REDIS
bool MasterMetaRedisStore::EnsureConnectedLocked(ConnSlot& slot) {
    if (slot.ctx != nullptr) return true;
    auto conn = ha::common::redis::ConnectRedis(config_.connstring);
    if (!conn) {
        LOG(ERROR) << "MetaRedis: connect failed";
        return false;
    }
    slot.ctx = std::move(conn.value());
    return true;
}

bool MasterMetaRedisStore::HSet(ConnSlot& slot, std::string_view hash_key,
                                std::string_view field,
                                std::string_view value) {
    if (!EnsureConnectedLocked(slot)) return false;
    ha::common::redis::RedisReplyPtr reply(
        static_cast<redisReply*>(redisCommand(
            slot.ctx.get(), "HSET %b %b %b", hash_key.data(),
            hash_key.size(), field.data(), field.size(), value.data(),
            value.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "MetaRedis HSET failed, key=" << hash_key
                   << ", dropping connection for reconnect";
        slot.ctx.reset();
        return false;
    }
    return true;
}

bool MasterMetaRedisStore::DelObjects(
    size_t bucket, const std::vector<std::string>& fields) {
    if (fields.empty() || !Enabled()) return true;
    ConnSlot& slot = SlotForBucket(bucket);
    std::vector<std::string> owned_fields(fields);
    return Submit(slot, [this, bucket, fields = std::move(owned_fields)](
                             ConnSlot& s) {
        return DoDelObjects(s, bucket, fields);
    });
}

bool MasterMetaRedisStore::DoDelObjects(
    ConnSlot& slot, size_t bucket, const std::vector<std::string>& fields) {
    if (!EnsureConnectedLocked(slot)) return false;
    const std::string hash_key = ObjHashKey(bucket);
    // Chunk size MOONCAKE_META_REDIS_HDEL_CHUNK_FIELDS, 256 matches the eviction flusher batch.
    const size_t max_fields_per_cmd =
        config_.hdel_chunk_fields > 0 ? config_.hdel_chunk_fields : 1;
    for (size_t off = 0; off < fields.size(); off += max_fields_per_cmd) {
        const size_t n = std::min(max_fields_per_cmd, fields.size() - off);
        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        argv.reserve(n + 2);
        argvlen.reserve(n + 2);
        argv.push_back("HDEL");
        argvlen.push_back(4);
        argv.push_back(hash_key.data());
        argvlen.push_back(hash_key.size());
        for (size_t i = 0; i < n; ++i) {
            argv.push_back(fields[off + i].data());
            argvlen.push_back(fields[off + i].size());
        }
        ha::common::redis::RedisReplyPtr reply(static_cast<redisReply*>(
            redisCommandArgv(slot.ctx.get(), static_cast<int>(argv.size()),
                             argv.data(), argvlen.data())));
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "MetaRedis batch HDEL failed, key=" << hash_key
                       << ", fields=" << n
                       << ", dropping connection for reconnect";
            slot.ctx.reset();
            return false;
        }
    }
    return true;
}

bool MasterMetaRedisStore::PutObjects(
    size_t bucket,
    const std::vector<std::pair<std::string, std::string_view>>&
        field_values) {
    if (field_values.empty() || !Enabled()) return true;
    ConnSlot& slot = SlotForBucket(bucket);
    std::vector<std::pair<std::string, std::string>> owned;
    owned.reserve(field_values.size());
    for (const auto& [field, value] : field_values) {
        owned.emplace_back(field, std::string(value));
    }
    return Submit(slot, [this, bucket, values = std::move(owned)](
                             ConnSlot& s) {
        return DoPutObjects(s, bucket, values);
    });
}

bool MasterMetaRedisStore::DoPutObjects(
    ConnSlot& slot, size_t bucket,
    const std::vector<std::pair<std::string, std::string>>& field_values) {
    if (!EnsureConnectedLocked(slot)) return false;
    const std::string hash_key = ObjHashKey(bucket);
    const size_t max_fields_per_cmd =
        config_.hdel_chunk_fields > 0 ? config_.hdel_chunk_fields : 1;
    for (size_t off = 0; off < field_values.size(); off += max_fields_per_cmd) {
        const size_t n = std::min(max_fields_per_cmd, field_values.size() - off);
        std::vector<const char*> argv;
        std::vector<size_t> argvlen;
        argv.reserve(2 * n + 2);
        argvlen.reserve(2 * n + 2);
        argv.push_back("HSET");
        argvlen.push_back(4);
        argv.push_back(hash_key.data());
        argvlen.push_back(hash_key.size());
        for (size_t i = 0; i < n; ++i) {
            const auto& [field, value] = field_values[off + i];
            argv.push_back(field.data());
            argvlen.push_back(field.size());
            argv.push_back(value.data());
            argvlen.push_back(value.size());
        }
        ha::common::redis::RedisReplyPtr reply(static_cast<redisReply*>(
            redisCommandArgv(slot.ctx.get(), static_cast<int>(argv.size()),
                             argv.data(), argvlen.data())));
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "MetaRedis batch HSET failed, key=" << hash_key
                       << ", fields=" << n
                       << ", dropping connection for reconnect";
            slot.ctx.reset();
            return false;
        }
    }
    return true;
}

bool MasterMetaRedisStore::HDel(ConnSlot& slot, std::string_view hash_key,
                                std::string_view field) {
    if (!EnsureConnectedLocked(slot)) return false;
    ha::common::redis::RedisReplyPtr reply(
        static_cast<redisReply*>(redisCommand(slot.ctx.get(), "HDEL %b %b",
                                              hash_key.data(), hash_key.size(),
                                              field.data(), field.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "MetaRedis HDEL failed, key=" << hash_key
                   << ", dropping connection for reconnect";
        slot.ctx.reset();
        return false;
    }
    return true;
}

bool MasterMetaRedisStore::PutObject(size_t bucket, std::string_view tenant_id,
                                     std::string_view key,
                                     std::string_view value) {
    if (!Enabled()) return true;
    ConnSlot& slot = SlotForBucket(bucket);
    std::string hash_key = ObjHashKey(bucket);
    std::string field = EncodeField(tenant_id, key);
    std::string owned_value(value);
    return Submit(slot, [this, hash_key = std::move(hash_key),
                         field = std::move(field),
                         value = std::move(owned_value)](ConnSlot& s) {
        return HSet(s, hash_key, field, value);
    });
}

bool MasterMetaRedisStore::DelObject(size_t bucket, std::string_view tenant_id,
                                     std::string_view key) {
    if (!Enabled()) return true;
    ConnSlot& slot = SlotForBucket(bucket);
    std::string hash_key = ObjHashKey(bucket);
    std::string field = EncodeField(tenant_id, key);
    return Submit(slot, [this, hash_key = std::move(hash_key),
                         field = std::move(field)](ConnSlot& s) {
        return HDel(s, hash_key, field);
    });
}

bool MasterMetaRedisStore::PutSegment(const StandbySegmentInfo& segment) {
    if (!Enabled()) return true;
    const std::string hash_key = SegHashKey();
    const auto bytes = struct_pack::serialize(segment);
    std::string value(reinterpret_cast<const char*>(bytes.data()),
                      bytes.size());
    std::string field = segment.transport_endpoint;
    // Segments have no bucket; route them all to slot 0 so repeated writes of
    // the same endpoint stay ordered on one actor.
    ConnSlot& slot = *conns_[0];
    return Submit(slot, [this, hash_key, field = std::move(field),
                         value = std::move(value)](ConnSlot& s) {
        return HSet(s, hash_key, field, value);
    });
}

bool MasterMetaRedisStore::DelSegment(std::string_view transport_endpoint) {
    if (!Enabled()) return true;
    const std::string hash_key = SegHashKey();
    std::string field(transport_endpoint);
    ConnSlot& slot = *conns_[0];
    return Submit(slot, [this, hash_key, field = std::move(field)](
                            ConnSlot& s) { return HDel(s, hash_key, field); });
}

#else  // STORE_USE_REDIS

// Stubs for builds without hiredis. Writes are no-ops that report success so
// the master runs normally.

bool MasterMetaRedisStore::PutObject(size_t, std::string_view, std::string_view,
                                     std::string_view) {
    return true;
}

bool MasterMetaRedisStore::DelObject(size_t, std::string_view,
                                     std::string_view) {
    return true;
}

bool MasterMetaRedisStore::DelObjects(
    size_t, const std::vector<std::string>&) {
    return true;
}

bool MasterMetaRedisStore::PutObjects(
    size_t,
    const std::vector<std::pair<std::string, std::string_view>>&) {
    return true;
}


bool MasterMetaRedisStore::PutSegment(const StandbySegmentInfo&) { return true; }

bool MasterMetaRedisStore::DelSegment(std::string_view) { return true; }

#endif  // STORE_USE_REDIS

}  // namespace mooncake
