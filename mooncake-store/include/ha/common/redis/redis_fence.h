#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

namespace mooncake::ha::common::redis {

// Per-hash leader fencing for the metadata mirror.
//
// A serving leader claims a monotonically increasing epoch (`INCR <ns>:epoch`)
// and stamps it into every mirror hash as `__mc_epoch` before serving. Every
// mirror mutation then runs a single-key EVAL that rejects the write unless
// the hash still carries the writer's epoch. A stale leader that resumes after
// a freeze (or a partitioned leader) therefore cannot mutate Redis even before
// its own lease monitor notices the loss.
//
// The stamp lives in the same hash as the data, so each EVAL touches exactly
// one key and stays valid on Redis Cluster / proxies where cross-slot EVAL is
// rejected.
inline constexpr const char* kFenceField = "__mc_epoch";

inline constexpr const char* kFencedHSetScript = R"lua(
if redis.call('HGET', KEYS[1], '__mc_epoch') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
return 1
)lua";

inline constexpr const char* kFencedHDelScript = R"lua(
if redis.call('HGET', KEYS[1], '__mc_epoch') ~= ARGV[1] then return 0 end
redis.call('HDEL', KEYS[1], ARGV[2])
return 1
)lua";

// ARGV[1] = epoch, then (field, value) pairs.
inline constexpr const char* kFencedBatchHSetScript = R"lua(
if redis.call('HGET', KEYS[1], '__mc_epoch') ~= ARGV[1] then return 0 end
for i = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
)lua";

// ARGV[1] = epoch, then fields.
inline constexpr const char* kFencedBatchHDelScript = R"lua(
if redis.call('HGET', KEYS[1], '__mc_epoch') ~= ARGV[1] then return 0 end
for i = 2, #ARGV do
  redis.call('HDEL', KEYS[1], ARGV[i])
end
return 1
)lua";

// Monotonic stamp: never lowers an epoch already written by a newer leader.
inline constexpr const char* kStampEpochScript = R"lua(
local cur = tonumber(redis.call('HGET', KEYS[1], '__mc_epoch') or '0')
local new = tonumber(ARGV[1])
if cur <= new then
  redis.call('HSET', KEYS[1], '__mc_epoch', ARGV[1])
end
return 1
)lua";

#ifdef STORE_USE_REDIS
// EVAL <script> 1 <key> <args...>. All arguments are binary-safe.
inline redisReply* EvalScript(redisContext* ctx, const char* script,
                              std::string_view key,
                              std::initializer_list<std::string_view> args) {
    static const char kEval[] = "EVAL";
    static const char kOne[] = "1";
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size() + 4);
    argvlen.reserve(args.size() + 4);
    argv.push_back(kEval);
    argvlen.push_back(sizeof(kEval) - 1);
    argv.push_back(script);
    argvlen.push_back(std::strlen(script));
    argv.push_back(kOne);
    argvlen.push_back(sizeof(kOne) - 1);
    argv.push_back(key.data());
    argvlen.push_back(key.size());
    for (const auto arg : args) {
        argv.push_back(arg.data());
        argvlen.push_back(arg.size());
    }
    return static_cast<redisReply*>(redisCommandArgv(
        ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
}

inline redisReply* EvalScript(redisContext* ctx, const char* script,
                              std::string_view key,
                              const std::vector<std::string_view>& args) {
    static const char kEval[] = "EVAL";
    static const char kOne[] = "1";
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size() + 4);
    argvlen.reserve(args.size() + 4);
    argv.push_back(kEval);
    argvlen.push_back(sizeof(kEval) - 1);
    argv.push_back(script);
    argvlen.push_back(std::strlen(script));
    argv.push_back(kOne);
    argvlen.push_back(sizeof(kOne) - 1);
    argv.push_back(key.data());
    argvlen.push_back(key.size());
    for (const auto arg : args) {
        argv.push_back(arg.data());
        argvlen.push_back(arg.size());
    }
    return static_cast<redisReply*>(redisCommandArgv(
        ctx, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
}

inline bool IsFenceMatch(const redisReply* reply) {
    return reply != nullptr && reply->type == REDIS_REPLY_INTEGER &&
           reply->integer == 1;
}

inline bool IsFenceMismatch(const redisReply* reply) {
    return reply != nullptr && reply->type == REDIS_REPLY_INTEGER &&
           reply->integer == 0;
}
#endif

}  // namespace mooncake::ha::common::redis
