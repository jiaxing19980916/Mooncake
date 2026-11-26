#include "ha/kv/master_meta_redis_store.h"

#include "ha/common/redis/redis_fence.h"

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

std::string MasterMetaRedisStore::EpochKey() const {
    const std::string prefix =
        config_.ns.empty() ? std::string() : config_.ns + ":";
    return prefix + "epoch";
}

#ifdef STORE_USE_REDIS
namespace {
// Stamp one epoch into a list of hashes in bounded pipelines.
bool StampHashes(redisContext* ctx, const std::vector<std::string>& hashes,
                 const std::string& epoch, const char* what) {
    constexpr size_t kChunk = 128;
    for (size_t off = 0; off < hashes.size(); off += kChunk) {
        const size_t n = std::min(kChunk, hashes.size() - off);
        size_t appended = 0;
        for (size_t i = 0; i < n; ++i) {
            const std::string& hash_key = hashes[off + i];
            static const char kEval[] = "EVAL";
            static const char kOne[] = "1";
            std::vector<const char*> argv = {
                kEval, ha::common::redis::kStampEpochScript, kOne,
                hash_key.data(), epoch.data()};
            std::vector<size_t> argvlen = {
                sizeof(kEval) - 1,
                std::strlen(ha::common::redis::kStampEpochScript),
                sizeof(kOne) - 1, hash_key.size(), epoch.size()};
            if (redisAppendCommandArgv(ctx, static_cast<int>(argv.size()),
                                       argv.data(), argvlen.data()) !=
                REDIS_OK) {
                LOG(ERROR) << "MetaRedis epoch stamp append failed, key="
                           << hash_key;
                return false;
            }
            ++appended;
        }
        for (size_t i = 0; i < appended; ++i) {
            redisReply* reply = nullptr;
            if (redisGetReply(ctx, reinterpret_cast<void**>(&reply)) !=
                    REDIS_OK ||
                reply == nullptr || reply->type != REDIS_REPLY_INTEGER) {
                if (reply != nullptr) freeReplyObject(reply);
                LOG(ERROR) << "MetaRedis epoch stamp failed for " << what;
                return false;
            }
            freeReplyObject(reply);
        }
    }
    return true;
}
}  // namespace
#endif

bool MasterMetaRedisStore::StartLeaderEpoch() {
    if (!Enabled()) return true;
#ifndef STORE_USE_REDIS
    LOG(ERROR) << "MetaRedis leader epoch requested but the build lacks "
                  "STORE_USE_REDIS";
    return false;
#else
    auto conn = ha::common::redis::ConnectRedis(config_.connstring);
    if (!conn) {
        LOG(ERROR) << "MetaRedis epoch claim: connect failed";
        return false;
    }
    redisContext* ctx = conn.value().get();
    ha::common::redis::RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(ctx, "INCR %s", EpochKey().c_str())));
    if (reply == nullptr || reply->type != REDIS_REPLY_INTEGER ||
        reply->integer <= 0) {
        LOG(ERROR) << "MetaRedis epoch claim: INCR failed, key="
                   << EpochKey();
        return false;
    }
    fence_epoch_ = static_cast<uint64_t>(reply->integer);
    fence_lost_.store(false, std::memory_order_relaxed);

    // Stamp every hash this store can write. A stale leader's per-key EVAL
    // compares against this field, so a hash without the stamp fails closed.
    std::vector<std::string> hashes;
    hashes.reserve(config_.num_buckets + 1);
    for (size_t b = 0; b < config_.num_buckets; ++b) {
        hashes.push_back(ObjHashKey(b));
    }
    hashes.push_back(SegHashKey());
    const std::string epoch = std::to_string(fence_epoch_);
    if (!StampHashes(ctx, hashes, epoch, "object/segment hashes")) {
        return false;
    }
    LOG(INFO) << "MetaRedis leader epoch claimed: " << fence_epoch_;
    return true;
#endif
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
    if (fence_epoch_ != 0) {
        const std::string epoch = std::to_string(fence_epoch_);
        ha::common::redis::RedisReplyPtr reply(ha::common::redis::EvalScript(
            slot.ctx.get(), ha::common::redis::kFencedHSetScript, hash_key,
            {epoch, field, value}));
        if (ha::common::redis::IsFenceMismatch(reply.get())) {
            fence_lost_.store(true, std::memory_order_relaxed);
            LOG(ERROR) << "MetaRedis leader fence lost (HSET), key=" << hash_key;
            return false;
        }
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "MetaRedis fenced HSET failed, key=" << hash_key
                       << ", dropping connection for reconnect";
            slot.ctx.reset();
            return false;
        }
        return ha::common::redis::IsFenceMatch(reply.get());
    }
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
        ha::common::redis::RedisReplyPtr reply;
        if (fence_epoch_ != 0) {
            const std::string epoch = std::to_string(fence_epoch_);
            std::vector<std::string_view> args;
            args.reserve(n + 1);
            args.push_back(epoch);
            for (size_t i = 0; i < n; ++i) {
                args.push_back(fields[off + i]);
            }
            reply.reset(ha::common::redis::EvalScript(
                slot.ctx.get(), ha::common::redis::kFencedBatchHDelScript,
                hash_key, args));
            if (ha::common::redis::IsFenceMismatch(reply.get())) {
                fence_lost_.store(true, std::memory_order_relaxed);
                LOG(ERROR) << "MetaRedis leader fence lost (batch HDEL), key="
                           << hash_key;
                return false;
            }
        } else {
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
            reply.reset(static_cast<redisReply*>(
                redisCommandArgv(slot.ctx.get(), static_cast<int>(argv.size()),
                                 argv.data(), argvlen.data())));
        }
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
        ha::common::redis::RedisReplyPtr reply;
        if (fence_epoch_ != 0) {
            const std::string epoch = std::to_string(fence_epoch_);
            std::vector<std::string_view> args;
            args.reserve(2 * n + 1);
            args.push_back(epoch);
            for (size_t i = 0; i < n; ++i) {
                args.push_back(field_values[off + i].first);
                args.push_back(field_values[off + i].second);
            }
            reply.reset(ha::common::redis::EvalScript(
                slot.ctx.get(), ha::common::redis::kFencedBatchHSetScript,
                hash_key, args));
            if (ha::common::redis::IsFenceMismatch(reply.get())) {
                fence_lost_.store(true, std::memory_order_relaxed);
                LOG(ERROR) << "MetaRedis leader fence lost (batch HSET), key="
                           << hash_key;
                return false;
            }
        } else {
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
            reply.reset(static_cast<redisReply*>(
                redisCommandArgv(slot.ctx.get(), static_cast<int>(argv.size()),
                                 argv.data(), argvlen.data())));
        }
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

bool MasterMetaRedisStore::PutObjectAndHashes(
    size_t bucket, std::string_view tenant_id, std::string_view key,
    std::string_view payload, const std::vector<HSetCmd>& extra) {
    if (!Enabled()) return true;
    ConnSlot& slot = SlotForBucket(bucket);
    std::string owned_tenant(tenant_id);
    std::string owned_key(key);
    std::string owned_payload(payload);
    std::vector<HSetCmd> owned_extra(extra);
    return Submit(
        slot,
        [this, bucket, tenant = std::move(owned_tenant),
         key = std::move(owned_key), payload = std::move(owned_payload),
         extra = std::move(owned_extra)](ConnSlot& s) {
            return DoPutObjectAndHashes(s, bucket, tenant, key, payload, extra);
        });
}

bool MasterMetaRedisStore::DoPutObjectAndHashes(
    ConnSlot& slot, size_t bucket, std::string_view tenant_id,
    std::string_view key, std::string_view payload,
    const std::vector<HSetCmd>& extra) {
    if (!EnsureConnectedLocked(slot)) return false;
    const std::string hash_key = ObjHashKey(bucket);
    const std::string field = EncodeField(tenant_id, key);
    const std::string epoch = std::to_string(fence_epoch_);
    // Pipeline only no multi key EVAL, obj oth may hit different slots CROSSSLOT.
    // When the leader fence is on, each command is a single-key EVAL against
    // its own hash; the whole batch is still one network round trip.
    // Ledger first, object last, a pipeline that dies mid way then leaves at most orphan oth
    // records safe, reclaimed later, never an object whose extents are missing from the ledger.
    auto append_write = [&](std::string_view hk, std::string_view f,
                            std::string_view v) -> bool {
        if (fence_epoch_ == 0) {
            return redisAppendCommand(slot.ctx.get(), "HSET %b %b %b",
                                      hk.data(), hk.size(), f.data(), f.size(),
                                      v.data(), v.size()) == REDIS_OK;
        }
        static const char kEval[] = "EVAL";
        static const char kOne[] = "1";
        std::vector<const char*> argv = {kEval,
                                         ha::common::redis::kFencedHSetScript,
                                         kOne,
                                         hk.data(),
                                         epoch.data(),
                                         f.data(),
                                         v.data()};
        std::vector<size_t> argvlen = {
            sizeof(kEval) - 1,
            std::strlen(ha::common::redis::kFencedHSetScript),
            sizeof(kOne) - 1,
            hk.size(),
            epoch.size(),
            f.size(),
            v.size()};
        return redisAppendCommandArgv(slot.ctx.get(),
                                      static_cast<int>(argv.size()),
                                      argv.data(), argvlen.data()) == REDIS_OK;
    };

    size_t appended = 0;
    bool ok = true;
    for (const auto& cmd : extra) {
        if (!append_write(cmd.hash_key, cmd.field, cmd.value)) {
            ok = false;
            break;
        }
        ++appended;
    }
    if (ok) {
        // binary form everywhere, EncodeField is binary safe, string form would truncate the field at a NUL.
        if (append_write(hash_key, field, payload)) {
            ++appended;
        } else {
            ok = false;
        }
    }
    bool fence_rejected = false;
    for (size_t i = 0; i < appended; ++i) {
        redisReply* reply = nullptr;
        if (redisGetReply(slot.ctx.get(), reinterpret_cast<void**>(&reply)) !=
                REDIS_OK ||
            reply == nullptr) {
            ok = false;
            break;
        }
        if (fence_epoch_ != 0) {
            if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
                fence_rejected = true;
                ok = false;
            } else if (reply->type != REDIS_REPLY_INTEGER) {
                ok = false;
            }
        } else if (reply->type == REDIS_REPLY_ERROR) {
            ok = false;
        }
        freeReplyObject(reply);
    }
    if (!ok) {
        if (fence_rejected) {
            fence_lost_.store(true, std::memory_order_relaxed);
            LOG(ERROR) << "MetaRedis leader fence lost (pipeline HSET), key="
                       << hash_key;
        } else {
            LOG(ERROR) << "MetaRedis pipelined HSET failed, key=" << hash_key
                       << ", extra=" << extra.size()
                       << ", dropping connection for reconnect";
            slot.ctx.reset();
        }
    }
    return ok;
}

bool MasterMetaRedisStore::HDel(ConnSlot& slot, std::string_view hash_key,
                                std::string_view field) {
    if (!EnsureConnectedLocked(slot)) return false;
    if (fence_epoch_ != 0) {
        const std::string epoch = std::to_string(fence_epoch_);
        ha::common::redis::RedisReplyPtr reply(ha::common::redis::EvalScript(
            slot.ctx.get(), ha::common::redis::kFencedHDelScript, hash_key,
            {epoch, field}));
        if (ha::common::redis::IsFenceMismatch(reply.get())) {
            fence_lost_.store(true, std::memory_order_relaxed);
            LOG(ERROR) << "MetaRedis leader fence lost (HDEL), key=" << hash_key;
            return false;
        }
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "MetaRedis fenced HDEL failed, key=" << hash_key
                       << ", dropping connection for reconnect";
            slot.ctx.reset();
            return false;
        }
        return ha::common::redis::IsFenceMatch(reply.get());
    }
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

// ---- read side (leader promotion) ------------------------------------------

tl::expected<std::vector<StandbyObjectEntry>, ErrorCode>
MasterMetaRedisStore::RecoverObjects() {
    if (!Enabled()) return std::vector<StandbyObjectEntry>{};
    if (config_.pscan_shards > 1) {
        return RecoverObjectsConcurrent(config_.pscan_shards);
    }
    // Sequential path uses a private connection: slot 0 is owned by its actor.
    auto conn = ha::common::redis::ConnectRedis(config_.connstring);
    if (!conn) return tl::make_unexpected(conn.error());
    redisContext* ctx = conn.value().get();
    std::vector<StandbyObjectEntry> out;
    for (size_t b = 0; b < config_.num_buckets; ++b) {
        auto restored = RecoverOneObjectHash(ctx, ObjHashKey(b), out);
        if (!restored) return tl::make_unexpected(restored.error());
    }
    LOG(INFO) << "MetaRedis restored " << out.size()
              << " objects from Redis (sequential)";
    return out;
}

tl::expected<std::vector<StandbySegmentInfo>, ErrorCode>
MasterMetaRedisStore::RecoverSegments() {
    if (!Enabled()) return std::vector<StandbySegmentInfo>{};
    std::vector<StandbySegmentInfo> out;
    const std::string hash_key = SegHashKey();
    auto conn = ha::common::redis::ConnectRedis(config_.connstring);
    if (!conn) return tl::make_unexpected(conn.error());
    redisContext* ctx = conn.value().get();
    ha::common::redis::RedisReplyPtr reply(
        static_cast<redisReply*>(redisCommand(ctx, "HGETALL %s",
                                              hash_key.c_str())));
    if (reply == nullptr || reply->type != REDIS_REPLY_ARRAY) {
        LOG(ERROR) << "MetaRedis HGETALL failed, key=" << hash_key;
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    for (size_t i = 0; i + 1 < reply->elements; i += 2) {
        redisReply* f = reply->element[i];
        redisReply* v = reply->element[i + 1];
        if (std::string_view(f->str, f->len) ==
            ha::common::redis::kFenceField) {
            continue;
        }
        StandbySegmentInfo seg;
        if (struct_pack::deserialize_to(
                seg, std::string_view(v->str, v->len)) !=
            struct_pack::errc::ok) {
            LOG(ERROR) << "MetaRedis restore: corrupt segment value in "
                       << hash_key;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        out.push_back(std::move(seg));
    }
    LOG(INFO) << "MetaRedis restored " << out.size() << " segments from Redis";
    return out;
}

tl::expected<uint64_t, ErrorCode> MasterMetaRedisStore::RecoverOneObjectHash(
    redisContext* ctx, const std::string& hash_key,
    std::vector<StandbyObjectEntry>& out) {
    uint64_t restored = 0;
    std::string cursor = "0";
    do {
        ha::common::redis::RedisReplyPtr reply(
            static_cast<redisReply*>(redisCommand(ctx, "HSCAN %s %s COUNT 1000",
                                                  hash_key.c_str(),
                                                  cursor.c_str())));
        if (reply == nullptr || reply->type != REDIS_REPLY_ARRAY ||
            reply->elements != 2) {
            LOG(ERROR) << "MetaRedis HSCAN failed, key=" << hash_key;
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
        cursor.assign(reply->element[0]->str, reply->element[0]->len);
        redisReply* kv = reply->element[1];
        for (size_t i = 0; i + 1 < kv->elements; i += 2) {
            redisReply* f = kv->element[i];
            redisReply* v = kv->element[i + 1];
            std::string_view field(f->str, f->len);
            if (field == ha::common::redis::kFenceField) {
                continue;
            }
            std::string tenant_id;
            std::string key;
            if (!DecodeField(field, tenant_id, key)) {
                LOG(ERROR) << "MetaRedis restore: bad field in " << hash_key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            MetadataPayload payload;
            if (struct_pack::deserialize_to(
                    payload, std::string_view(v->str, v->len)) !=
                struct_pack::errc::ok) {
                LOG(ERROR) << "MetaRedis restore: corrupt value in " << hash_key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            StandbyObjectEntry entry;
            entry.tenant_id = std::move(tenant_id);
            entry.key = std::move(key);
            entry.metadata = payload.ToStandbyMetadata();
            out.push_back(std::move(entry));
            ++restored;
        }
    } while (cursor != "0");
    return restored;
}

tl::expected<std::vector<StandbyObjectEntry>, ErrorCode>
MasterMetaRedisStore::RecoverObjectsConcurrent(int num_pshards) {
    const std::string pattern = ObjHashPrefix() + "*";
    std::vector<tl::expected<std::vector<StandbyObjectEntry>, ErrorCode>>
        results(num_pshards,
                tl::expected<std::vector<StandbyObjectEntry>, ErrorCode>(
                    std::vector<StandbyObjectEntry>{}));
    std::vector<std::thread> threads;
    threads.reserve(num_pshards);

    for (int p = 0; p < num_pshards; ++p) {
        threads.emplace_back([this, p, &pattern, &results]() {
            results[p] =
                [&]() -> tl::expected<std::vector<StandbyObjectEntry>,
                                      ErrorCode> {
                auto conn = ha::common::redis::ConnectRedis(config_.connstring);
                if (!conn) {
                    LOG(ERROR) << "MetaRedis restore(PSCAN): connect failed for "
                                  "physical shard "
                               << p;
                    return tl::make_unexpected(conn.error());
                }
                redisContext* ctx = conn.value().get();

                // Discover the obj:* hashes living on this physical shard.
                // SCAN may return duplicates across iterations, so dedupe.
                std::unordered_set<std::string> hash_keys;
                std::string cursor = "0";
                do {
                    ha::common::redis::RedisReplyPtr reply(
                        static_cast<redisReply*>(redisCommand(
                            ctx, "PSCAN %d %s MATCH %s COUNT 1000", p,
                            cursor.c_str(), pattern.c_str())));
                    if (reply == nullptr ||
                        reply->type != REDIS_REPLY_ARRAY ||
                        reply->elements != 2) {
                        LOG(ERROR) << "MetaRedis restore: PSCAN failed, "
                                      "physical shard="
                                   << p;
                        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
                    }
                    cursor.assign(reply->element[0]->str,
                                  reply->element[0]->len);
                    redisReply* arr = reply->element[1];
                    for (size_t i = 0; i < arr->elements; ++i) {
                        redisReply* k = arr->element[i];
                        if (k != nullptr && k->str != nullptr) {
                            hash_keys.emplace(k->str, k->len);
                        }
                    }
                } while (cursor != "0");

                std::vector<StandbyObjectEntry> local;
                for (const auto& hk : hash_keys) {
                    if (ParseBucketFromHashKey(hk) < 0) {
                        LOG(WARNING) << "MetaRedis restore: ignoring "
                                        "unrecognized key on physical shard "
                                     << p << ": " << hk;
                        continue;
                    }
                    auto restored = RecoverOneObjectHash(ctx, hk, local);
                    if (!restored) {
                        return tl::make_unexpected(restored.error());
                    }
                }
                return local;
            }();
        });
    }
    for (auto& t : threads) t.join();

    std::vector<StandbyObjectEntry> out;
    std::optional<ErrorCode> first_error;
    for (auto& r : results) {
        if (!r) {
            if (!first_error) first_error = r.error();
        } else {
            for (auto& e : *r) out.push_back(std::move(e));
        }
    }
    if (first_error) {
        LOG(ERROR) << "MetaRedis restore(PSCAN): aborting, first error="
                   << static_cast<int>(*first_error);
        return tl::make_unexpected(*first_error);
    }
    LOG(INFO) << "MetaRedis restored " << out.size()
              << " objects from Redis (concurrent PSCAN, physical shards="
              << num_pshards << ")";
    return out;
}

#else  // STORE_USE_REDIS

// Stubs for builds without hiredis. Writes are no-ops that report success so
// the master runs normally; recovery reads only succeed when Redis recovery is
// disabled, otherwise they surface PERSISTENT_FAIL (misconfiguration).

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

bool MasterMetaRedisStore::PutObjectAndHashes(
    size_t, std::string_view, std::string_view, std::string_view,
    const std::vector<HSetCmd>&) {
    return true;
}

bool MasterMetaRedisStore::PutSegment(const StandbySegmentInfo&) { return true; }

bool MasterMetaRedisStore::DelSegment(std::string_view) { return true; }

tl::expected<std::vector<StandbyObjectEntry>, ErrorCode>
MasterMetaRedisStore::RecoverObjects() {
    if (!Enabled()) return std::vector<StandbyObjectEntry>{};
    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
}

tl::expected<std::vector<StandbySegmentInfo>, ErrorCode>
MasterMetaRedisStore::RecoverSegments() {
    if (!Enabled()) return std::vector<StandbySegmentInfo>{};
    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
}

#endif  // STORE_USE_REDIS

}  // namespace mooncake
