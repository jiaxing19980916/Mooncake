// Unit tests for MasterMetaRedisStore's pure key/field codec and hash-key
// builders (no Redis required). The put/recover round-trip against live Redis
// is covered by an integration test, mirroring dfs_redis_recovery_test.cpp.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ha/kv/master_meta_redis_store.h"

namespace mooncake {
namespace {

MasterMetaRedisStore MakeStore(const std::string& ns, size_t num_buckets) {
    MasterMetaRedisStore::Config cfg;
    cfg.ns = ns;
    cfg.num_buckets = num_buckets;
    // connstring stays empty (store disabled): the constructor never connects,
    // so this is safe to build in a unit test.
    return MasterMetaRedisStore(std::move(cfg));
}

TEST(MasterMetaRedisStoreCodec, EncodeDecodeRoundTrip) {
    struct Case {
        std::string tenant;
        std::string key;
    };
    const std::string binary_key = std::string("k\x00:\x01/end", 8);
    const Case cases[] = {
        {"default", "simple/key"},
        {"", "empty-tenant-key"},
        {"tenant-1", ""},
        {"t", binary_key},          // key with embedded NUL and ':'
        {"tenant:with:colons", "a:b:c"},
    };
    for (const auto& c : cases) {
        const std::string field =
            MasterMetaRedisStore::EncodeField(c.tenant, c.key);
        std::string tenant_out;
        std::string key_out;
        ASSERT_TRUE(MasterMetaRedisStore::DecodeField(field, tenant_out,
                                                      key_out))
            << "tenant=" << c.tenant;
        EXPECT_EQ(tenant_out, c.tenant);
        EXPECT_EQ(key_out, c.key);
    }
}

TEST(MasterMetaRedisStoreCodec, DecodeRejectsShortField) {
    std::string tenant_out;
    std::string key_out;
    EXPECT_FALSE(MasterMetaRedisStore::DecodeField("abc", tenant_out, key_out));
    // 4-hex length says 5 bytes of tenant but only 2 remain.
    EXPECT_FALSE(
        MasterMetaRedisStore::DecodeField("0005ab", tenant_out, key_out));
    // Non-hex length prefix.
    EXPECT_FALSE(
        MasterMetaRedisStore::DecodeField("zzzzkey", tenant_out, key_out));
}

TEST(MasterMetaRedisStoreKeys, ObjHashKeyRoundTripsThroughParse) {
    auto store = MakeStore("mc1", 1024);
    EXPECT_EQ(store.ObjHashPrefix(), "mc1:obj:");
    EXPECT_EQ(store.ObjHashKey(7), "mc1:obj:0007");  // width 4 for 1024 buckets
    EXPECT_EQ(store.ObjHashKey(1023), "mc1:obj:1023");
    for (size_t b : {size_t{0}, size_t{7}, size_t{512}, size_t{1023}}) {
        EXPECT_EQ(store.ParseBucketFromHashKey(store.ObjHashKey(b)),
                  static_cast<int>(b));
    }
}

TEST(MasterMetaRedisStoreKeys, ParseRejectsForeignAndOutOfRange) {
    auto store = MakeStore("mc1", 1024);
    EXPECT_EQ(store.ParseBucketFromHashKey("mc1:seg"), -1);
    EXPECT_EQ(store.ParseBucketFromHashKey("other:obj:0007"), -1);
    EXPECT_EQ(store.ParseBucketFromHashKey("mc1:obj:"), -1);
    EXPECT_EQ(store.ParseBucketFromHashKey("mc1:obj:abcd"), -1);
    EXPECT_EQ(store.ParseBucketFromHashKey("mc1:obj:1024"), -1);  // >= num_buckets
}

TEST(MasterMetaRedisStoreKeys, EmptyNamespaceOmitsPrefix) {
    auto store = MakeStore("", 1024);
    EXPECT_EQ(store.ObjHashPrefix(), "obj:");
    EXPECT_EQ(store.ObjHashKey(3), "obj:0003");
    EXPECT_EQ(store.SegHashKey(), "seg");
    EXPECT_EQ(store.ParseBucketFromHashKey("obj:0003"), 3);
}

TEST(MasterMetaRedisStoreKeys, SegHashKeyIsNamespaced) {
    auto store = MakeStore("mc1", 1024);
    EXPECT_EQ(store.SegHashKey(), "mc1:seg");
}

// Opt-in smoke test for the per-slot actor path. Set
// MOONCAKE_META_REDIS_TEST_CONNSTRING (e.g. redis://127.0.0.1:6380) to run.
TEST(MasterMetaRedisStoreLive, ConcurrentActorsKeepEveryWrite) {
    const char* connstring = std::getenv("MOONCAKE_META_REDIS_TEST_CONNSTRING");
    if (connstring == nullptr || *connstring == '\0') {
        GTEST_SKIP() << "MOONCAKE_META_REDIS_TEST_CONNSTRING is not set";
    }

    MasterMetaRedisStore::Config cfg;
    cfg.connstring = connstring;
    cfg.ns = "actor-smoke-" +
             std::to_string(std::chrono::system_clock::now()
                                .time_since_epoch()
                                .count());
    cfg.num_buckets = 64;
    cfg.write_conns = 4;
    MasterMetaRedisStore store(std::move(cfg));

    constexpr int kThreads = 8;
    constexpr int kKeysPerThread = 200;
    const std::string tenant = "default";
    std::atomic<int> succeeded{0};
    std::atomic<int> failed{0};

    std::vector<std::thread> writers;
    writers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&store, t, &tenant, &succeeded, &failed] {
            for (int i = 0; i < kKeysPerThread; ++i) {
                const size_t bucket =
                    static_cast<size_t>(t * kKeysPerThread + i) % 64;
                const std::string key =
                    "actor-" + std::to_string(t) + "-" + std::to_string(i);
                if (store.PutObject(bucket, tenant, key, "payload")) {
                    succeeded.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failed.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& writer : writers) writer.join();

    EXPECT_EQ(failed.load(), 0);
    EXPECT_EQ(succeeded.load(), kThreads * kKeysPerThread);

    for (size_t bucket = 0; bucket < 64; ++bucket) {
        std::vector<std::string> fields;
        for (int t = 0; t < kThreads; ++t) {
            for (int i = 0; i < kKeysPerThread; ++i) {
                if (static_cast<size_t>(t * kKeysPerThread + i) % 64 != bucket) {
                    continue;
                }
                fields.push_back(MasterMetaRedisStore::EncodeField(
                    tenant, "actor-" + std::to_string(t) + "-" +
                                std::to_string(i)));
            }
        }
        if (!fields.empty()) {
            EXPECT_TRUE(store.DelObjects(bucket, fields));
        }
    }
}

// A stale leader must not be able to mutate the mirror after a newer leader
// has claimed the epoch. This is the fencing guarantee that prevents a frozen
// leader's late PutEnd / client-expiry deletes from poisoning Redis.
}  // namespace
}  // namespace mooncake
