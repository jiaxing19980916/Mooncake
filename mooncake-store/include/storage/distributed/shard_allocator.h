#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "offset_allocator/offset_allocator.h"
#include "replica.h"
#include "ha/common/redis/redis_connection.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/dfs_allocator_interface.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

class ShardAllocator final : public DfsAllocatorInterface {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx;
        uint64_t offset;
    };

    // Keeps selected allocations pinned while the master decides which
    // candidates can be evicted. An unresolved transaction is aborted on
    // destruction so candidates cannot get stuck outside the LRU.
    class PendingEviction {
       public:
        PendingEviction() = default;
        ~PendingEviction();

        PendingEviction(const PendingEviction&) = delete;
        PendingEviction& operator=(const PendingEviction&) = delete;
        PendingEviction(PendingEviction&& other) noexcept;
        PendingEviction& operator=(PendingEviction&&) = delete;

        bool Empty() const { return candidates_.empty(); }
        const std::vector<EvictionCandidate>& Candidates() const {
            return candidates_;
        }

       private:
        friend class ShardAllocator;

        struct PreparedAllocation {
            EvictionCandidate candidate;
            std::shared_ptr<offset_allocator::OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
        };

        explicit PendingEviction(ShardAllocator* owner) : owner_(owner) {}

        ShardAllocator* owner_ = nullptr;
        std::vector<EvictionCandidate> candidates_;
        std::vector<PreparedAllocation> prepared_;
    };

    ShardAllocator() = default;
    ~ShardAllocator() override;

    ShardAllocator(const ShardAllocator&) = delete;
    ShardAllocator& operator=(const ShardAllocator&) = delete;

    DfsAllocatorType Type() const override { return DfsAllocatorType::SHARD; }

    tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) override;
    bool IsInitialized() const override {
        return initialized_.load(std::memory_order_acquire);
    }

    // Append shards without moving existing allocations. Equal counts are
    // idempotent; shrinking is not supported. The returned count is ready for
    // allocation only after every new shard has been initialized.
    tl::expected<int, ErrorCode> ExpandShards(int target_count);
    int GetShardCount() const;

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;
    std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>& requests) override;
    void Free(const std::string& key,
              const DistributedFSDescriptor& descriptor) override;
    void UpdateAccess(const std::string& key,
                      const DistributedFSDescriptor& descriptor) override;
    void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
              const std::string& key);
    void UpdateAccess(const std::string& key, int shard_idx, uint64_t offset);
    PendingEviction PrepareEviction();
    void CommitPreparedEviction(PendingEviction&& pending);
    void RestorePreparedEviction(PendingEviction&& pending);
    void ResolvePreparedEviction(PendingEviction&& pending,
                                 const std::vector<bool>& accepted);

    bool IsEvictionEnabled() const override { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const override {
        return eviction_check_interval_;
    }
    uint64_t GetUsedBytes() const override;
    uint64_t GetTotalCapacity() const override;
    uint64_t GetFileCount() const override;

    // ===== Redis persistence of offset_to_handle (single writer: master) =====
    // Builds one oth:<shard> record without Redis so PutEnd can pipeline it
    // with the object metadata in one round trip. Returns false when the
    // allocation is no longer present under the shard lock.
    bool PrepareCommitRecord(int shard_idx, uint64_t alloc_offset,
                             std::string& hash_key, std::string& field,
                             std::string& value) const;
    // HSET one allocation record. True on success/no-op, false only on error.
    bool CommitAllocation(int shard_idx, uint64_t alloc_offset);
    // HDEL one allocation record, used by PutEnd rollback.
    void UncommitAllocation(int shard_idx, uint64_t alloc_offset);

    static std::string FormatShardIdx(int idx, int shard_count);

   private:
    using OffsetAllocator = offset_allocator::OffsetAllocator;
    using OffsetAllocationHandle = offset_allocator::OffsetAllocationHandle;

    struct ShardState {
        std::string path;
        uint64_t capacity = 0;
        std::shared_ptr<OffsetAllocator> allocator;

        struct AllocationRecord {
            std::string key;
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
            uint64_t object_size = 0;  // original value length for Redis
            bool eviction_prepared = false;
            // Set while Free() HDELs the ledger outside the shard locks. The
            // extent stays allocated (so the offset cannot be reused) and is
            // skipped by UpdateAccess / eviction / a second Free.
            bool free_prepared = false;
        };

        std::shared_mutex handle_mutex;
        std::unordered_map<uint64_t, AllocationRecord> offset_to_handle;

        std::mutex lru_mutex;
        std::list<std::pair<std::string, uint64_t>> lru_list;
        // Keys view the key stored in their lru_list node, so each tracked
        // key is stored once. Insert only with that node's key (never
        // operator[] with a caller's string), and erase an index entry
        // before its list node.
        std::unordered_map<std::string_view, decltype(lru_list)::iterator>
            lru_index;
        // Once the high watermark is crossed, keep selecting candidates until
        // effective usage falls below the low watermark. Protected candidates
        // may make that span multiple prepare/resolve rounds.
        bool eviction_active = false;

        struct PendingFree {
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
            std::chrono::steady_clock::time_point when;
        };
        std::mutex pending_mutex;
        std::deque<PendingFree> pending_free;
        uint64_t pending_free_bytes = 0;
    };

    using ShardList = std::vector<std::shared_ptr<ShardState>>;

    tl::expected<std::shared_ptr<ShardState>, ErrorCode> CreateShard(
        const std::string& path, std::vector<std::string>& created_files);
    void CleanupCreatedFiles(const std::vector<std::string>& created_files);

    static constexpr size_t kNumKeyStripes = 65536;

    std::unique_lock<std::mutex> LockKey(const std::string& key) {
        return std::unique_lock<std::mutex>(
            key_stripes_[std::hash<std::string>{}(key) % kNumKeyStripes]);
    }

    void QueuePendingFree(ShardState& shard,
                          const std::shared_ptr<OffsetAllocationHandle>& handle,
                          uint64_t bytes,
                          std::chrono::steady_clock::time_point when);
    void CleanupExpiredPendingFrees(ShardState& shard,
                                    std::chrono::steady_clock::time_point now);
    double EffectiveUsage(ShardState& shard);
    void PrepareEvictionFromShard(ShardState& shard, int shard_idx,
                                  PendingEviction& pending);
    uint64_t AlignSize(uint64_t size) const;

    // ===== Redis ledger helpers =====
    std::string ShardHashPrefix() const;
    std::string ShardHashKey(int shard_idx) const;
    int ParseShardIdxFromHashKey(const std::string& hash_key) const;
    bool RedisPersistRecord(int shard_idx, const std::string& hash_key,
                            const std::string& field,
                            const std::string& value);
    bool RedisRemoveAlloc(int shard_idx, uint64_t alloc_offset);

    std::string mount_path_;
    uint64_t shard_capacity_ = 0;
    uint64_t alignment_ = 4096;
    // Readers use an immutable topology snapshot. Expansion shares existing
    // ShardState objects and publishes the complete new topology in one step.
    // Readers keep no reference, so superseded snapshots are never freed:
    // published_shards_ (guarded by expansion_mutex_) holds every snapshot
    // until destruction. Only Init and a growing ExpandShards publish, so each
    // expansion retains 16 bytes (one shared_ptr) per shard.
    std::atomic<const ShardList*> shards_{nullptr};
    std::vector<std::unique_ptr<const ShardList>> published_shards_;
    std::mutex expansion_mutex_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds deferred_free_duration_{30};
    std::chrono::seconds eviction_check_interval_{5};
    std::atomic<bool> initialized_{false};
    std::array<std::mutex, kNumKeyStripes> key_stripes_;

    // Per-shard Redis actors, striped by shard index. Each connection is owned
    // by one I/O thread; callers enqueue and wait on a promise, so the queue
    // mutex never covers a network RTT.
    bool redis_enabled_ = false;
    std::string redis_connstring_;
    std::string redis_namespace_;
    int redis_pscan_shards_ = 0;
    int redis_write_conns_ = 0;
    struct RedisConn;
    struct RedisJob {
        std::function<bool(RedisConn&)> run;
        std::promise<bool> done;
    };
    struct RedisConn {
        static constexpr size_t kQueueLimit = 1024;

        std::mutex mutex;
        std::condition_variable work_cv;
        std::condition_variable space_cv;
        std::deque<RedisJob> queue;
        std::thread worker;
        bool stopping = false;
        // Actor-only after Init; no other thread touches ctx.
        ha::common::redis::RedisContextPtr ctx;
    };
    std::vector<std::unique_ptr<RedisConn>> redis_conns_;
#ifdef STORE_USE_REDIS
    RedisConn& RedisConnForShard(int shard_idx) {
        return *redis_conns_[static_cast<size_t>(shard_idx) %
                             redis_conns_.size()];
    }
    bool Submit(RedisConn& conn, std::function<bool(RedisConn&)> job);
    void ActorLoop(RedisConn& conn);
    bool EnsureRedisConnected(RedisConn& conn);
    bool DoRedisPersistRecord(RedisConn& conn, const std::string& hash_key,
                              const std::string& field,
                              const std::string& value);
    bool DoRedisRemoveAlloc(RedisConn& conn, const std::string& hash_key,
                            const std::string& field);
#endif
};

}  // namespace mooncake
