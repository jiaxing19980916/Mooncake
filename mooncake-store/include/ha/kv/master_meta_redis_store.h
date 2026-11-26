#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/common/redis/redis_connection.h"
#include "metadata_store.h"  // StandbyObjectEntry, StandbySegmentInfo
#include "types.h"           // ErrorCode

struct redisContext;

namespace mooncake {

// Redis mirror of the master's object table + segment registry that rebuilds
// in-memory metadata on leader failover (snapshot/oplog off). Driven by
// MOONCAKE_ENABLE_REDIS_RECOVERY; kept strongly consistent via write-ahead gates.
class MasterMetaRedisStore {
   public:
    struct Config {
        std::string connstring;     // empty => store disabled
        std::string ns;             // key namespace, = cluster_id
        size_t num_buckets = 1024;  // MUST match MasterService shard count
        // Number of Redis connections writes are striped across (keyed by
        // object bucket). <=1 => a single connection (legacy behavior). >1 lets
        // a sharded Redis / proxy process writes to different buckets in
        // parallel instead of serializing them all on one connection.
        int write_conns = 0;
        // Max fields per batched HDEL, 256 matches the eviction flusher batch one round trip per group.
        size_t hdel_chunk_fields = 256;
    };

    explicit MasterMetaRedisStore(Config config);
    ~MasterMetaRedisStore();

    // Enabled iff a connstring was supplied. The master expresses the single
    // switch MOONCAKE_ENABLE_REDIS_RECOVERY by only constructing this store
    // with a non-empty connstring.
    bool Enabled() const { return !config_.connstring.empty(); }

    // ---- write side (caller already holds the shard lock) ----
    // `value` is the struct_pack(MetadataPayload) blob, stored opaque and
    // decoded back on recovery. Returns true on success (or when disabled) and
    // false on HSET failure, so the caller can gate its write-ahead commit.
    bool PutObject(size_t bucket, std::string_view tenant_id,
                   std::string_view key, std::string_view value);
    // Returns true on success (or when the store is disabled -> no-op; note HDEL
    // of an absent field is still success) and false only on a Redis error, so a
    // delete path can gate the in-memory removal on the Redis HDEL.
    bool DelObject(size_t bucket, std::string_view tenant_id,
                   std::string_view key);
    // Multi field HDEL for one bucket chunked, collapses the eviction flushers per key deletes.
    bool DelObjects(size_t bucket, const std::vector<std::string>& fields);
    // Multi field HSET for one bucket chunked, batches the eviction flushers per key trims.
    bool PutObjects(
        size_t bucket,
        const std::vector<std::pair<std::string, std::string_view>>&
            field_values);
    // Returns true on success (or when disabled -> no-op), false on HSET
    // failure, so the caller can gate mount success on the segment being
    // mirrored (symmetric to PutObject's write-ahead gate).
    bool PutSegment(const StandbySegmentInfo& segment);
    // Returns true on success (or when disabled -> no-op), false on HDEL
    // failure, so unmount can retry while the segment is still in memory.
    bool DelSegment(std::string_view transport_endpoint);

    // ---- key/field codec (public for unit tests) ----
    // field = hex4(tenant.size()) + tenant + key  (fixed-length prefix keeps it
    // binary-safe and collision-free for arbitrary key bytes).
    static std::string EncodeField(std::string_view tenant_id,
                                   std::string_view key);
    static bool DecodeField(std::string_view field, std::string& tenant_id,
                            std::string& key);
    std::string ObjHashPrefix() const;             // "<ns>:obj:"
    std::string ObjHashKey(size_t bucket) const;   // "<ns>:obj:<bucket>"
    std::string SegHashKey() const;                // "<ns>:seg"
    int ParseBucketFromHashKey(std::string_view hash_key) const;

   private:
    static std::string FormatBucket(size_t bucket, size_t num_buckets);

    // Per-slot actor: one dedicated I/O thread owns one hiredis context. Callers
    // enqueue a job and wait on its promise, so the queue mutex never covers a
    // network RTT. Per-slot FIFO keeps the old per-connection ordering: a key
    // always maps to the same bucket, hence the same slot and actor.
    struct ConnSlot;
    struct Job {
        std::function<bool(ConnSlot&)> run;
        std::promise<bool> done;
    };
    struct ConnSlot {
        static constexpr size_t kQueueLimit = 1024;

        std::mutex mutex;
        std::condition_variable work_cv;
        std::condition_variable space_cv;
        std::deque<Job> queue;
        std::thread worker;
        bool stopping = false;
        // Actor-only after construction; no other thread touches ctx.
        ha::common::redis::RedisContextPtr ctx;
    };

#ifdef STORE_USE_REDIS
    ConnSlot& SlotForBucket(size_t bucket) const {
        return *conns_[bucket % conns_.size()];
    }
    // Queue one job on its slot and wait for the actor's result.
    bool Submit(ConnSlot& slot, std::function<bool(ConnSlot&)> job);
    void ActorLoop(ConnSlot& slot);
    bool EnsureConnectedLocked(ConnSlot& slot);  // actor thread only
    bool HSet(ConnSlot& slot, std::string_view hash_key, std::string_view field,
              std::string_view value);
    bool HDel(ConnSlot& slot, std::string_view hash_key,
              std::string_view field);
    bool DoDelObjects(ConnSlot& slot, size_t bucket,
                      const std::vector<std::string>& fields);
    bool DoPutObjects(
        ConnSlot& slot, size_t bucket,
        const std::vector<std::pair<std::string, std::string>>& field_values);
#endif

    Config config_;
    std::vector<std::unique_ptr<ConnSlot>> conns_;
};

}  // namespace mooncake
