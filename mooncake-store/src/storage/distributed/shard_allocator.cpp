#include "storage/distributed/shard_allocator.h"

#include <algorithm>
#include <charconv>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <string_view>
#include <utility>

#include "config/distributed_storage_config.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/posix_fs_adapter.h"
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#include <ylt/struct_pack.hpp>
#endif
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

namespace {
// Persisted per-allocation ledger record: field=alloc_offset, value=this.
struct DfsAllocRecord {
    uint64_t raw_offset = 0;
    uint64_t object_size = 0;
    std::string key;
    YLT_REFL(DfsAllocRecord, raw_offset, object_size, key);
};
}  // namespace

ShardAllocator::PendingEviction::~PendingEviction() {
    if (owner_ != nullptr) {
        owner_->RestorePreparedEviction(std::move(*this));
    }
}

ShardAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      candidates_(std::move(other.candidates_)),
      prepared_(std::move(other.prepared_)) {}

ShardAllocator::~ShardAllocator() {
#ifdef STORE_USE_REDIS
    for (auto& conn : redis_conns_) {
        {
            std::lock_guard<std::mutex> lock(conn->mutex);
            conn->stopping = true;
        }
        conn->work_cv.notify_all();
        conn->space_cv.notify_all();
    }
    for (auto& conn : redis_conns_) {
        if (conn->worker.joinable()) conn->worker.join();
    }
#endif
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> ShardAllocator::Init(
    const DistributedStorageConfig& config) {
    std::lock_guard expansion_lock(expansion_mutex_);
    if (initialized_.load(std::memory_order_acquire)) return {};
    if (!config.ValidateForAllocator() ||
        config.shard_capacity >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        config.shard_capacity > std::numeric_limits<uint64_t>::max() /
                                    static_cast<uint64_t>(config.shard_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    mount_path_ = config.fsdir;
    shard_capacity_ = config.shard_capacity;
    alignment_ = config.alignment;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    deferred_free_duration_ = config.deferred_free_duration;
    eviction_check_interval_ = config.eviction_check_interval;

    redis_connstring_ = config.redis_connstring;
    redis_namespace_ =
        ha::common::redis::SanitizeHashTagComponent(config.redis_namespace);
    redis_enabled_ = !redis_connstring_.empty();
    redis_pscan_shards_ = config.redis_pscan_shards;
    redis_write_conns_ = config.redis_write_conns;
    const size_t redis_conn_count =
        redis_write_conns_ > 0 ? static_cast<size_t>(redis_write_conns_) : 1;
    redis_conns_.clear();
    redis_conns_.reserve(redis_conn_count);
    for (size_t i = 0; i < redis_conn_count; ++i) {
        redis_conns_.push_back(std::make_unique<RedisConn>());
    }
#ifdef STORE_USE_REDIS
    if (redis_enabled_) {
        for (auto& conn : redis_conns_) {
            conn->worker = std::thread([this, conn = conn.get()] {
                ActorLoop(*conn);
            });
        }
    }
#endif

    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS mount path " << mount_path_ << ": "
                   << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (fs_adapter_) fs_adapter_->Shutdown();
    if (config.fs_adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "The hf3fs DFS adapter requires Mooncake to be built "
                      "with the USE_3FS compile-time option "
                      "(-DUSE_3FS=ON)";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    }

    auto adapter_init = fs_adapter_->Init(mount_path_);
    if (!adapter_init) {
        LOG(ERROR) << "Failed to initialize DFS fs adapter "
                   << config.fs_adapter_type
                   << " for mount_path=" << mount_path_
                   << ", error=" << adapter_init.error();
        return tl::make_unexpected(adapter_init.error());
    }

    // Recover the capacity layout only. Allocation handles and key metadata
    // remain empty; snapshot/oplog recovery is deliberately still unsupported.
    // Keep the exact paths of older layouts whose padding depended on count.
    auto files = fs_adapter_->ListFiles(mount_path_);
    if (!files) return tl::make_unexpected(files.error());
    std::map<int, std::string> existing_paths;
    constexpr std::string_view prefix = "dfs_shard_";
    constexpr std::string_view suffix = ".data";
    for (const auto& name : *files) {
        std::string_view view(name);
        if (!view.starts_with(prefix) || !view.ends_with(suffix)) continue;
        const auto digits = view.substr(
            prefix.size(), view.size() - prefix.size() - suffix.size());
        int index = -1;
        const auto [end, error] = std::from_chars(
            digits.data(), digits.data() + digits.size(), index);
        if (digits.size() < 2 || digits.front() < '0' || digits.front() > '9' ||
            error != std::errc{} || end != digits.data() + digits.size() ||
            index < 0 || index == std::numeric_limits<int>::max() ||
            !existing_paths.emplace(index, mount_path_ + "/" + name).second) {
            LOG(ERROR) << "Invalid or ambiguous DFS shard filename " << name;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int discovered_count = 0;
    for (const auto& [index, path] : existing_paths) {
        if (index != discovered_count++) {
            LOG(ERROR) << "DFS shard layout is not contiguous at " << path;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    const int target_count = std::max(config.shard_count, discovered_count);
    if (shard_capacity_ > std::numeric_limits<uint64_t>::max() /
                              static_cast<uint64_t>(target_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<std::string> created_files;
    try {
        auto ready = std::make_unique<ShardList>();
        ready->reserve(target_count);
        for (int i = 0; i < target_count; ++i) {
            const auto existing = existing_paths.find(i);
            const std::string path = existing == existing_paths.end()
                                         ? mount_path_ + "/dfs_shard_" +
                                               FormatShardIdx(i, 0) + ".data"
                                         : existing->second;
            auto shard = CreateShard(path, created_files);
            if (!shard) {
                CleanupCreatedFiles(created_files);
                return tl::make_unexpected(shard.error());
            }
            ready->push_back(std::move(*shard));
        }
        published_shards_.push_back(std::move(ready));
        shards_.store(published_shards_.back().get(),
                      std::memory_order_release);
    } catch (const std::exception& error) {
        CleanupCreatedFiles(created_files);
        LOG(ERROR) << "Failed to initialize DFS shards: " << error.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    initialized_.store(true, std::memory_order_release);
    return {};
}

int ShardAllocator::GetShardCount() const {
    const auto* shards = shards_.load(std::memory_order_acquire);
    return shards ? static_cast<int>(shards->size()) : 0;
}

tl::expected<int, ErrorCode> ShardAllocator::ExpandShards(int target_count) {
    std::lock_guard expansion_lock(expansion_mutex_);
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    const auto* current = shards_.load(std::memory_order_acquire);
    if (target_count <= 0 || target_count < static_cast<int>(current->size()) ||
        shard_capacity_ > std::numeric_limits<uint64_t>::max() /
                              static_cast<uint64_t>(target_count)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (target_count == static_cast<int>(current->size())) return target_count;

    std::vector<std::string> created_files;
    try {
        auto ready = std::make_unique<ShardList>();
        ready->reserve(target_count);
        ready->assign(current->begin(), current->end());
        for (int i = static_cast<int>(current->size()); i < target_count; ++i) {
            const std::string path =
                mount_path_ + "/dfs_shard_" + FormatShardIdx(i, 0) + ".data";
            auto shard = CreateShard(path, created_files);
            if (!shard) {
                // Files from before this operation are never truncated or
                // removed. Failed cleanup can leave a ready file for retry,
                // but no staged shard is allocatable before publication.
                CleanupCreatedFiles(created_files);
                return tl::make_unexpected(shard.error());
            }
            ready->push_back(std::move(*shard));
        }
        published_shards_.push_back(std::move(ready));
        shards_.store(published_shards_.back().get(),
                      std::memory_order_release);
    } catch (const std::exception& error) {
        CleanupCreatedFiles(created_files);
        LOG(ERROR) << "Failed to expand DFS shards: " << error.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return target_count;
}

tl::expected<std::shared_ptr<ShardAllocator::ShardState>, ErrorCode>
ShardAllocator::CreateShard(const std::string& path,
                            std::vector<std::string>& created_files) {
    auto exists = fs_adapter_->FileExists(path);
    if (!exists) return tl::make_unexpected(exists.error());
    if (*exists) {
        std::error_code ec;
        if (!std::filesystem::is_regular_file(path, ec)) {
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        auto size = fs_adapter_->GetFileSize(path);
        if (!size) return tl::make_unexpected(size.error());
        if (*size != shard_capacity_) {
            LOG(ERROR) << "DFS shard capacity mismatch for " << path
                       << ": expected=" << shard_capacity_
                       << ", actual=" << *size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    } else {
        created_files.push_back(path);
        auto prealloc = fs_adapter_->PreallocateFile(path, shard_capacity_);
        if (!prealloc) return tl::make_unexpected(prealloc.error());
    }

    auto shard = std::make_shared<ShardState>();
    shard->path = path;
    shard->capacity = shard_capacity_;
    uint32_t init_cap = static_cast<uint32_t>(std::max<uint64_t>(
        1, std::min<uint64_t>(shard_capacity_ / 4096, 64ULL * 1024)));
    uint32_t max_cap = static_cast<uint32_t>(std::max<uint64_t>(
        init_cap,
        std::min<uint64_t>(shard_capacity_ / 1024, 64ULL * 1024 * 1024)));
    shard->allocator =
        OffsetAllocator::create(0, shard_capacity_, init_cap, max_cap);
    if (!shard->allocator) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return shard;
}

void ShardAllocator::CleanupCreatedFiles(
    const std::vector<std::string>& created_files) {
    for (const auto& path : created_files) {
        auto result = fs_adapter_->DeleteFile(path);
        if (!result && result.error() != ErrorCode::FILE_NOT_FOUND) {
            LOG(WARNING) << "Failed to clean up unpublished DFS shard " << path
                         << ": " << result.error();
        }
    }
}

tl::expected<DistributedFSDescriptor, ErrorCode> ShardAllocator::Allocate(
    const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0 ||
        size > std::numeric_limits<uint64_t>::max() - (alignment_ - 1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // No leading slack is needed: each shard allocator starts at offset 0 and
    // only receives alignment-multiple requests, whose size-class rounding
    // stays a multiple of the power-of-two alignment. Every returned offset is
    // therefore already aligned.
    const uint64_t aligned_size = AlignSize(size);
    if (aligned_size > shard_capacity_) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto key_lock = LockKey(key);
    const auto* shards = shards_.load(std::memory_order_acquire);
    const size_t preferred = std::hash<std::string>{}(key) % shards->size();
    // A full preferred shard must not hide capacity in newly appended shards.
    // Existing descriptors remain authoritative even when the modulo changes.
    for (size_t attempt = 0; attempt < shards->size(); ++attempt) {
        const size_t shard_idx = (preferred + attempt) % shards->size();
        auto& shard = *(*shards)[shard_idx];
        std::unique_lock handle_lock(shard.handle_mutex);
        CleanupExpiredPendingFrees(shard, std::chrono::steady_clock::now());
        const uint64_t reserved_bytes =
            shard.allocator->normalizedAllocationSize(aligned_size);
        auto handle = shard.allocator->allocate(aligned_size);
        if (!handle) continue;

        const uint64_t alloc_offset = handle->address();
        if ((alloc_offset & (alignment_ - 1)) != 0) {
            // The reservation has no slack to round into; the handle releases
            // the extent when it goes out of scope.
            LOG(ERROR) << "DFS shard allocator returned unaligned offset "
                       << alloc_offset << " for alignment " << alignment_;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        auto alloc_handle =
            std::make_shared<OffsetAllocationHandle>(std::move(*handle));
        shard.offset_to_handle[alloc_offset] = {key, std::move(alloc_handle),
                                                reserved_bytes, size};
        return DistributedFSDescriptor{shard.path, alloc_offset, size,
                                       aligned_size,
                                       static_cast<int>(shard_idx)};
    }
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}

std::vector<BatchAllocateResult> ShardAllocator::BatchAllocate(
    const std::vector<BatchAllocateRequest>& requests) {
    std::vector<BatchAllocateResult> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back(
            BatchAllocateResult{request.key, {}, false, ErrorCode::OK});
    }

    for (size_t i = 0; i < requests.size(); ++i) {
        auto descriptor = Allocate(requests[i].key, requests[i].size);
        if (descriptor) {
            results[i].descriptor = std::move(*descriptor);
            results[i].success = true;
            continue;
        }

        const ErrorCode error = descriptor.error();
        for (size_t j = i; j > 0; --j) {
            auto& allocated = results[j - 1];
            if (allocated.success) {
                Free(allocated.key, allocated.descriptor);
            }
        }
        for (auto& result : results) {
            result.descriptor = {};
            result.success = false;
            result.error = error;
        }
        return results;
    }
    return results;
}

void ShardAllocator::Free(const std::string& key,
                          const DistributedFSDescriptor& descriptor) {
    Free(descriptor.offset, descriptor.aligned_size, descriptor.shard_idx, key);
}

void ShardAllocator::UpdateAccess(const std::string& key,
                                  const DistributedFSDescriptor& descriptor) {
    UpdateAccess(key, descriptor.shard_idx, descriptor.offset);
}

uint64_t ShardAllocator::GetUsedBytes() const {
    uint64_t used = 0;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (!shards) return 0;
    for (const auto& shard : *shards) {
        if (!shard) continue;
        std::shared_lock lock(shard->handle_mutex);
        const auto report = shard->allocator->storageReport();
        const uint64_t free_space =
            std::min<uint64_t>(report.totalFreeSpace, shard->capacity);
        used += shard->capacity - free_space;
    }
    return used;
}

uint64_t ShardAllocator::GetTotalCapacity() const {
    uint64_t total = 0;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (!shards) return 0;
    for (const auto& shard : *shards) {
        if (shard) total += shard->capacity;
    }
    return total;
}

uint64_t ShardAllocator::GetFileCount() const {
    return static_cast<uint64_t>(GetShardCount());
}

void ShardAllocator::Free(uint64_t offset, uint64_t /*aligned_size*/,
                          int shard_idx, const std::string& key) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (shard_idx < 0 || shard_idx >= static_cast<int>(shards->size())) return;

    auto& shard = *(*shards)[shard_idx];
    if (!redis_enabled_) {
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);
        auto it = shard.offset_to_handle.find(offset);
        if (it == shard.offset_to_handle.end()) return;
        if (it->second.key != key) return;
        if (it->second.eviction_prepared || it->second.free_prepared) return;
        auto lru_it = shard.lru_index.find(key);
        if (lru_it != shard.lru_index.end() &&
            lru_it->second->second == offset) {
            const auto list_it = lru_it->second;
            shard.lru_index.erase(lru_it);
            shard.lru_list.erase(list_it);
        }
        QueuePendingFree(
            shard, it->second.handle, it->second.bytes,
            std::chrono::steady_clock::now() + deferred_free_duration_);
        shard.offset_to_handle.erase(it);
        return;
    }

    std::shared_ptr<OffsetAllocationHandle> expected_handle;
    {
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);
        auto it = shard.offset_to_handle.find(offset);
        if (it == shard.offset_to_handle.end()) return;
        if (it->second.key != key) return;
        // A prepared eviction owns this extent until Resolve/Commit settles
        // it; a concurrent Free is already past the Redis step.
        if (it->second.eviction_prepared || it->second.free_prepared) return;
        // Keep the handle allocated while the lock is released, so the offset
        // cannot be reused before the ledger HDEL settles.
        it->second.free_prepared = true;
        expected_handle = it->second.handle;
    }

    // Redis first, outside the shard locks: a failed HDEL keeps the
    // allocation (and its LRU entry) so a restart cannot resurrect an extent
    // still marked live in memory.
    const bool hdel_ok = RedisRemoveAlloc(shard_idx, offset);

    std::lock_guard lru_lock(shard.lru_mutex);
    std::lock_guard handle_lock(shard.handle_mutex);
    auto it = shard.offset_to_handle.find(offset);
    if (it == shard.offset_to_handle.end() || it->second.key != key ||
        it->second.handle != expected_handle || !it->second.free_prepared) {
        // Another path retired it while the locks were released.
        return;
    }
    if (!hdel_ok) {
        it->second.free_prepared = false;
        LOG(ERROR) << "DFS Free: Redis HDEL failed, keeping allocation, shard="
                   << shard_idx << ", offset=" << offset;
        return;
    }

    auto lru_it = shard.lru_index.find(key);
    if (lru_it != shard.lru_index.end() && lru_it->second->second == offset) {
        const auto list_it = lru_it->second;
        shard.lru_index.erase(lru_it);
        shard.lru_list.erase(list_it);
    }
    QueuePendingFree(
        shard, it->second.handle, it->second.bytes,
        std::chrono::steady_clock::now() + deferred_free_duration_);
    shard.offset_to_handle.erase(it);
}

void ShardAllocator::UpdateAccess(const std::string& key, int shard_idx,
                                  uint64_t offset) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (shard_idx < 0 || shard_idx >= static_cast<int>(shards->size())) return;

    auto& shard = *(*shards)[shard_idx];
    std::lock_guard lru_lock(shard.lru_mutex);
    {
        std::shared_lock handle_lock(shard.handle_mutex);
        auto handle_it = shard.offset_to_handle.find(offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != key ||
            handle_it->second.eviction_prepared ||
            handle_it->second.free_prepared) {
            return;
        }
    }
    auto lru_it = shard.lru_index.find(key);
    if (lru_it != shard.lru_index.end()) {
        lru_it->second->second = offset;
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list,
                              lru_it->second);
    } else {
        shard.lru_list.push_front({key, offset});
        shard.lru_index.emplace(shard.lru_list.front().first,
                                shard.lru_list.begin());
    }
}

ShardAllocator::PendingEviction ShardAllocator::PrepareEviction() {
    PendingEviction pending(this);
    if (!initialized_.load(std::memory_order_acquire)) return pending;

    const auto* shards = shards_.load(std::memory_order_acquire);
    for (size_t i = 0; i < shards->size(); ++i) {
        auto& shard = *(*shards)[i];
        {
            std::unique_lock handle_lock(shard.handle_mutex);
            CleanupExpiredPendingFrees(shard, std::chrono::steady_clock::now());
        }
        PrepareEvictionFromShard(shard, static_cast<int>(i), pending);
    }
    return pending;
}

void ShardAllocator::CommitPreparedEviction(PendingEviction&& pending) {
    if (pending.owner_ != this) return;
    const auto* shards = shards_.load(std::memory_order_acquire);

    const auto free_at =
        std::chrono::steady_clock::now() + deferred_free_duration_;
    for (const auto& prepared : pending.prepared_) {
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);
        auto it = shard.offset_to_handle.find(prepared.candidate.offset);
        if (it == shard.offset_to_handle.end() ||
            it->second.key != prepared.candidate.key ||
            it->second.handle != prepared.handle) {
            // A concurrent metadata removal may already have called Free().
            continue;
        }
        if (redis_enabled_ &&
            !RedisRemoveAlloc(prepared.candidate.shard_idx,
                              prepared.candidate.offset)) {
            LOG(ERROR) << "DFS eviction: Redis HDEL failed, keeping extent, "
                       << "shard=" << prepared.candidate.shard_idx
                       << ", offset=" << prepared.candidate.offset;
            it->second.eviction_prepared = false;
            if (shard.lru_index.find(prepared.candidate.key) ==
                shard.lru_index.end()) {
                shard.lru_list.push_back(
                    {prepared.candidate.key, prepared.candidate.offset});
                shard.lru_index.emplace(shard.lru_list.back().first,
                                        std::prev(shard.lru_list.end()));
            }
            continue;
        }

        QueuePendingFree(shard, prepared.handle, prepared.bytes, free_at);
        shard.offset_to_handle.erase(it);
    }

    pending.prepared_.clear();
    pending.candidates_.clear();
    pending.owner_ = nullptr;
}

void ShardAllocator::RestorePreparedEviction(PendingEviction&& pending) {
    if (pending.owner_ != this) return;
    const auto* shards = shards_.load(std::memory_order_acquire);

    // Candidates were removed oldest-first. Restore in reverse order so the
    // original relative LRU order is preserved.
    for (auto prepared_it = pending.prepared_.rbegin();
         prepared_it != pending.prepared_.rend(); ++prepared_it) {
        const auto& prepared = *prepared_it;
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);

        auto handle_it = shard.offset_to_handle.find(prepared.candidate.offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != prepared.candidate.key ||
            handle_it->second.handle != prepared.handle) {
            // Free() or a replacement already retired this allocation.
            continue;
        }

        handle_it->second.eviction_prepared = false;
        if (shard.lru_index.find(prepared.candidate.key) ==
            shard.lru_index.end()) {
            shard.lru_list.push_back(
                {prepared.candidate.key, prepared.candidate.offset});
            shard.lru_index.emplace(shard.lru_list.back().first,
                                    std::prev(shard.lru_list.end()));
        }
    }

    pending.prepared_.clear();
    pending.candidates_.clear();
    pending.owner_ = nullptr;
}

void ShardAllocator::ResolvePreparedEviction(
    PendingEviction&& pending, const std::vector<bool>& accepted) {
    if (pending.owner_ != this) return;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (accepted.size() != pending.prepared_.size()) {
        LOG(ERROR) << "DFS eviction decision count " << accepted.size()
                   << " does not match prepared candidate count "
                   << pending.prepared_.size();
        RestorePreparedEviction(std::move(pending));
        return;
    }

    // HDEL the allocator ledger outside the shard locks. The prepared extent
    // keeps its handle allocated and Free() refuses to retire a prepared
    // extent, so the offset cannot be reused between validation and HDEL.
    std::vector<bool> hdel_ok(pending.prepared_.size(), !redis_enabled_);
    if (redis_enabled_) {
        std::vector<size_t> hdel_indexes;
        hdel_indexes.reserve(pending.prepared_.size());
        for (size_t i = 0; i < pending.prepared_.size(); ++i) {
            if (!accepted[i]) continue;
            const auto& prepared = pending.prepared_[i];
            auto& shard = *(*shards)[prepared.candidate.shard_idx];
            std::lock_guard lru_lock(shard.lru_mutex);
            std::lock_guard handle_lock(shard.handle_mutex);
            auto handle_it =
                shard.offset_to_handle.find(prepared.candidate.offset);
            if (handle_it == shard.offset_to_handle.end() ||
                handle_it->second.key != prepared.candidate.key ||
                handle_it->second.handle != prepared.handle) {
                continue;
            }
            hdel_indexes.push_back(i);
        }
        for (const size_t i : hdel_indexes) {
            const auto& prepared = pending.prepared_[i];
            if (!RedisRemoveAlloc(prepared.candidate.shard_idx,
                                  prepared.candidate.offset)) {
                LOG(ERROR) << "DFS eviction: Redis HDEL failed, keeping extent, "
                           << "shard=" << prepared.candidate.shard_idx
                           << ", offset=" << prepared.candidate.offset;
                continue;
            }
            hdel_ok[i] = true;
        }
    }

    const auto free_at =
        std::chrono::steady_clock::now() + deferred_free_duration_;
    for (size_t i = 0; i < pending.prepared_.size(); ++i) {
        if (!accepted[i]) continue;

        const auto& prepared = pending.prepared_[i];
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);
        auto handle_it = shard.offset_to_handle.find(prepared.candidate.offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != prepared.candidate.key ||
            handle_it->second.handle != prepared.handle) {
            // Free() or a replacement already retired this allocation.
            continue;
        }
        if (redis_enabled_ && !hdel_ok[i]) {
            handle_it->second.eviction_prepared = false;
            if (shard.lru_index.find(prepared.candidate.key) ==
                shard.lru_index.end()) {
                shard.lru_list.push_back(
                    {prepared.candidate.key, prepared.candidate.offset});
                shard.lru_index.emplace(shard.lru_list.back().first,
                                        std::prev(shard.lru_list.end()));
            }
            continue;
        }

        QueuePendingFree(shard, prepared.handle, prepared.bytes, free_at);
        shard.offset_to_handle.erase(handle_it);
    }

    // Candidates were removed oldest-first. Restore rejected entries in
    // reverse order so their relative LRU order is preserved.
    for (size_t i = pending.prepared_.size(); i > 0; --i) {
        if (accepted[i - 1]) continue;

        const auto& prepared = pending.prepared_[i - 1];
        auto& shard = *(*shards)[prepared.candidate.shard_idx];
        std::lock_guard lru_lock(shard.lru_mutex);
        std::lock_guard handle_lock(shard.handle_mutex);

        auto handle_it = shard.offset_to_handle.find(prepared.candidate.offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != prepared.candidate.key ||
            handle_it->second.handle != prepared.handle) {
            continue;
        }

        handle_it->second.eviction_prepared = false;
        if (shard.lru_index.find(prepared.candidate.key) ==
            shard.lru_index.end()) {
            shard.lru_list.push_back(
                {prepared.candidate.key, prepared.candidate.offset});
            shard.lru_index.emplace(shard.lru_list.back().first,
                                    std::prev(shard.lru_list.end()));
        }
    }

    pending.prepared_.clear();
    pending.candidates_.clear();
    pending.owner_ = nullptr;
}

std::string ShardAllocator::FormatShardIdx(int idx, int /*shard_count*/) {
    // Retain the second argument for source compatibility. New filenames are
    // independent of total count; discovered legacy filenames stay unchanged.
    constexpr int width = 2;
    std::ostringstream oss;
    oss << std::setw(width) << std::setfill('0') << idx;
    return oss.str();
}

void ShardAllocator::QueuePendingFree(
    ShardState& shard, const std::shared_ptr<OffsetAllocationHandle>& handle,
    uint64_t bytes, std::chrono::steady_clock::time_point when) {
    if (!handle) return;
    std::lock_guard pending_lock(shard.pending_mutex);
    if (bytes == 0) bytes = handle->size();
    shard.pending_free.push_back({handle, bytes, when});
    shard.pending_free_bytes += bytes;
}

void ShardAllocator::CleanupExpiredPendingFrees(
    ShardState& shard, std::chrono::steady_clock::time_point now) {
    std::lock_guard pending_lock(shard.pending_mutex);
    while (!shard.pending_free.empty() &&
           shard.pending_free.front().when <= now) {
        const uint64_t bytes = shard.pending_free.front().bytes;
        shard.pending_free.pop_front();
        if (bytes > shard.pending_free_bytes) {
            shard.pending_free_bytes = 0;
        } else {
            shard.pending_free_bytes -= bytes;
        }
    }
}

double ShardAllocator::EffectiveUsage(ShardState& shard) {
    uint64_t physical_free = 0;
    {
        std::shared_lock lock(shard.handle_mutex);
        auto report = shard.allocator->storageReport();
        physical_free = report.totalFreeSpace;
    }

    uint64_t pending_free_bytes = 0;
    {
        std::lock_guard pending_lock(shard.pending_mutex);
        pending_free_bytes = shard.pending_free_bytes;
    }

    const uint64_t capped_physical_free =
        std::min<uint64_t>(physical_free, shard.capacity);
    const uint64_t remaining_capacity = shard.capacity - capped_physical_free;
    const uint64_t effective_free =
        capped_physical_free + std::min(pending_free_bytes, remaining_capacity);
    if (shard.capacity == 0) return 0.0;
    return 1.0 - static_cast<double>(effective_free) /
                     static_cast<double>(shard.capacity);
}

void ShardAllocator::PrepareEvictionFromShard(ShardState& shard, int shard_idx,
                                              PendingEviction& pending) {
    const double usage = EffectiveUsage(shard);
    uint64_t prepared_bytes = 0;
    std::lock_guard lru_lock(shard.lru_mutex);
    std::lock_guard handle_lock(shard.handle_mutex);

    if (usage >= eviction_high_watermark_) {
        shard.eviction_active = true;
    }
    if (!shard.eviction_active) return;
    if (usage < eviction_low_watermark_) {
        shard.eviction_active = false;
        return;
    }

    while (true) {
        if (shard.lru_list.empty()) break;
        auto lru_it = std::prev(shard.lru_list.end());
        const std::string evict_key = lru_it->first;
        const uint64_t evict_offset = lru_it->second;

        auto handle_it = shard.offset_to_handle.find(evict_offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != evict_key) {
            shard.lru_index.erase(evict_key);
            shard.lru_list.erase(lru_it);
            continue;
        }
        if (handle_it->second.free_prepared) {
            // An in-flight Free() owns this extent. Stop this round rather
            // than dropping its LRU node: a failed HDEL keeps the allocation
            // and must keep its eviction entry too.
            break;
        }
        if (handle_it->second.eviction_prepared) {
            shard.lru_index.erase(evict_key);
            shard.lru_list.erase(lru_it);
            continue;
        }

        EvictionCandidate candidate{evict_key, shard_idx, evict_offset};
        PendingEviction::PreparedAllocation prepared{
            candidate, handle_it->second.handle, handle_it->second.bytes};
        pending.prepared_.push_back(std::move(prepared));
        try {
            pending.candidates_.push_back(std::move(candidate));
        } catch (...) {
            pending.prepared_.pop_back();
            throw;
        }

        handle_it->second.eviction_prepared = true;
        prepared_bytes += handle_it->second.bytes;
        shard.lru_index.erase(evict_key);
        shard.lru_list.erase(lru_it);

        const double projected_usage =
            usage - static_cast<double>(prepared_bytes) /
                        static_cast<double>(shard.capacity);
        if (projected_usage < eviction_low_watermark_) break;
    }
}

std::string ShardAllocator::ShardHashPrefix() const {
    const std::string prefix =
        redis_namespace_.empty() ? std::string() : redis_namespace_ + ":";
    return prefix + "oth:";
}

std::string ShardAllocator::ShardHashKey(int shard_idx) const {
    return ShardHashPrefix() + FormatShardIdx(shard_idx, GetShardCount());
}

int ShardAllocator::ParseShardIdxFromHashKey(
    const std::string& hash_key) const {
    const std::string prefix = ShardHashPrefix();
    if (hash_key.size() <= prefix.size()) return -1;
    if (hash_key.compare(0, prefix.size(), prefix) != 0) return -1;
    const std::string suffix = hash_key.substr(prefix.size());
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
    if (idx < 0 || idx >= GetShardCount()) return -1;
    return idx;
}

bool ShardAllocator::PrepareCommitRecord(int shard_idx, uint64_t alloc_offset,
                                         std::string& hash_key,
                                         std::string& field,
                                         std::string& value) const {
    if (!redis_enabled_) return false;
    if (!initialized_.load(std::memory_order_acquire)) return false;
    const auto* shards = shards_.load(std::memory_order_acquire);
    if (!shards || shard_idx < 0 ||
        shard_idx >= static_cast<int>(shards->size())) {
        return false;
    }
    auto& shard = *(*shards)[shard_idx];
    uint64_t raw_offset = 0;
    uint64_t object_size = 0;
    std::string key;
    {
        std::shared_lock lock(shard.handle_mutex);
        auto it = shard.offset_to_handle.find(alloc_offset);
        if (it == shard.offset_to_handle.end() || !it->second.handle ||
            it->second.free_prepared) {
            return false;
        }
        raw_offset = it->second.handle->address();
        object_size = it->second.object_size;
        key = it->second.key;
    }
    hash_key = ShardHashKey(shard_idx);
    field = std::to_string(alloc_offset);
#ifdef STORE_USE_REDIS
    DfsAllocRecord rec{raw_offset, object_size, std::move(key)};
    const auto serialized = struct_pack::serialize(rec);
    value.assign(serialized.begin(), serialized.end());
#else
    (void)raw_offset;
    (void)object_size;
    value.clear();
#endif
    return true;
}

bool ShardAllocator::CommitAllocation(int shard_idx, uint64_t alloc_offset) {
    if (!redis_enabled_) return true;
    std::string hash_key;
    std::string field;
    std::string value;
    if (!PrepareCommitRecord(shard_idx, alloc_offset, hash_key, field, value)) {
        // The record disappeared under the shard lock; not a Redis failure.
        return true;
    }
    return RedisPersistRecord(shard_idx, hash_key, field, value);
}

void ShardAllocator::UncommitAllocation(int shard_idx, uint64_t alloc_offset) {
    RedisRemoveAlloc(shard_idx, alloc_offset);
}

bool ShardAllocator::RedisPersistRecord(int shard_idx,
                                        const std::string& hash_key,
                                        const std::string& field,
                                        const std::string& value) {
    if (!redis_enabled_) return true;
#ifndef STORE_USE_REDIS
    (void)shard_idx;
    (void)hash_key;
    (void)field;
    (void)value;
    return true;
#else
    RedisConn& conn = RedisConnForShard(shard_idx);
    std::string owned_hash(hash_key);
    std::string owned_field(field);
    std::string owned_value(value);
    return Submit(conn,
                  [this, hash_key = std::move(owned_hash),
                   field = std::move(owned_field),
                   value = std::move(owned_value)](RedisConn& c) {
                      return DoRedisPersistRecord(c, hash_key, field, value);
                  });
#endif
}

bool ShardAllocator::RedisRemoveAlloc(int shard_idx, uint64_t alloc_offset) {
    if (!redis_enabled_) return true;
#ifndef STORE_USE_REDIS
    (void)shard_idx;
    (void)alloc_offset;
    return true;
#else
    std::string hash_key = ShardHashKey(shard_idx);
    std::string field = std::to_string(alloc_offset);
    RedisConn& conn = RedisConnForShard(shard_idx);
    return Submit(conn, [this, hash_key = std::move(hash_key),
                         field = std::move(field)](RedisConn& c) {
        return DoRedisRemoveAlloc(c, hash_key, field);
    });
#endif
}

#ifdef STORE_USE_REDIS
bool ShardAllocator::Submit(RedisConn& conn,
                            std::function<bool(RedisConn&)> job) {
    std::unique_lock<std::mutex> lock(conn.mutex);
    conn.space_cv.wait(lock, [&conn] {
        return conn.stopping || conn.queue.size() < RedisConn::kQueueLimit;
    });
    if (conn.stopping) return false;
    conn.queue.push_back(RedisJob{std::move(job), std::promise<bool>()});
    auto result = conn.queue.back().done.get_future();
    conn.work_cv.notify_one();
    lock.unlock();
    return result.get();
}

void ShardAllocator::ActorLoop(RedisConn& conn) {
    while (true) {
        RedisJob job;
        {
            std::unique_lock<std::mutex> lock(conn.mutex);
            conn.work_cv.wait(lock, [&conn] {
                return conn.stopping || !conn.queue.empty();
            });
            if (conn.queue.empty()) {
                if (conn.stopping) break;
                continue;
            }
            job = std::move(conn.queue.front());
            conn.queue.pop_front();
            conn.space_cv.notify_one();
        }
        bool ok = false;
        try {
            if (job.run) ok = job.run(conn);
        } catch (const std::exception& e) {
            LOG(ERROR) << "DFS redis actor job threw: " << e.what();
        } catch (...) {
            LOG(ERROR) << "DFS redis actor job threw an unknown exception";
        }
        job.done.set_value(ok);
    }
}

bool ShardAllocator::EnsureRedisConnected(RedisConn& conn) {
    if (conn.ctx != nullptr) return true;
    auto connection = ha::common::redis::ConnectRedis(redis_connstring_);
    if (!connection) {
        LOG(ERROR) << "DFS redis: connect failed";
        return false;
    }
    conn.ctx = std::move(connection.value());
    return true;
}

bool ShardAllocator::DoRedisPersistRecord(RedisConn& conn,
                                         const std::string& hash_key,
                                         const std::string& field,
                                         const std::string& value) {
    if (!EnsureRedisConnected(conn)) return false;
    ha::common::redis::RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(conn.ctx.get(), "HSET %s %s %b", hash_key.c_str(),
                     field.c_str(), value.data(), value.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "DFS redis HSET failed, key=" << hash_key
                   << " field=" << field
                   << ", dropping connection for reconnect";
        conn.ctx.reset();
        return false;
    }
    return true;
}

bool ShardAllocator::DoRedisRemoveAlloc(RedisConn& conn,
                                        const std::string& hash_key,
                                        const std::string& field) {
    if (!EnsureRedisConnected(conn)) return false;
    ha::common::redis::RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(conn.ctx.get(), "HDEL %s %s", hash_key.c_str(),
                     field.c_str())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "DFS redis HDEL failed, key=" << hash_key
                   << " field=" << field
                   << ", dropping connection for reconnect";
        conn.ctx.reset();
        return false;
    }
    return true;
}
#endif

uint64_t ShardAllocator::AlignSize(uint64_t size) const {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

}  // namespace mooncake
