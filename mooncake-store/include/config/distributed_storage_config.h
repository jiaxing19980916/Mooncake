#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    std::string allocator_type = "shard";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t bucket_capacity = 1ULL * 1024 * 1024 * 1024;
    int64_t max_bucket_count = 64;
    uint64_t alignment = 4096;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};
    std::string object_storage_config_path;

    // Redis persistence of the shard allocator's offset_to_handle ledger.
    // Empty connstring disables it; the master sets these only when
    // MOONCAKE_ENABLE_REDIS_RECOVERY is on. redis_namespace is the cluster id.
    std::string redis_connstring;
    std::string redis_namespace;
    int redis_pscan_shards = 0;
    int redis_write_conns = 0;
    // Leader epoch stamped into every shard hash. Non-zero turns on the
    // per-hash fence that rejects writes from a stale leader.
    uint64_t redis_fence_epoch = 0;

    bool UsesObjectStorage() const {
        return fs_adapter_type == "oss" || fs_adapter_type == "s3" ||
               fs_adapter_type == "kvcs-lowlevel";
    }
    bool UsesKvcs() const { return fs_adapter_type == "kvcs-lowlevel"; }

    bool Validate() const;
    bool ValidateForAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
