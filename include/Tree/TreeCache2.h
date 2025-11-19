#ifndef TREE_CACHE2_H
#define TREE_CACHE2_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

#include "../lruCache.h"

#include <unordered_map>
#include "FeatureStats.h" // 新增包含
#include <unordered_set>  // 新增包含
using namespace std;

class TreeCache2 : public AbsMethod
{
private:
    string myName_ = "TreeCache2";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;
    size_t cache2HitCount = 0;
    size_t flag1 = 0, flag2 = 0;

    size_t cache2AccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;

    // --- 新的FI-Cache成员 ---
    // 核心：Feature元数据
    unordered_map<uint64_t, FeatureStats> feature_stats_;
    // 实际缓存的chunk内容
    lru11::Cache<uint64_t, std::vector<uint8_t>> chunk_cache_;
    // 当前缓存中的Feature集合
    unordered_set<uint64_t> cached_features_;
    // 每个Feature树包含的chunks
    unordered_map<uint64_t, vector<uint64_t>> feature_tree_chunks_;

    const double IMPORTANCE_THRESHOLD = 5; // 可调参数：访问超过5次才考虑缓存
    const size_t CACHE_MAX_SIZE = 1024;    // 缓存中最大chunk数量
    const double EVICTION_RATIO = 0.1;     // 每次淘汰10%
    unordered_set<uint64_t> just_inserted_chunks_;
    // --- FI-Cache 私有方法 ---
    void update_feature_stats(uint64_t feature_hash, bool hit);
    bool should_cache_feature(uint64_t feature_hash);
    void load_feature_tree_to_cache(uint64_t root_chunk_id, uint64_t feature_hash);
    void evict_least_important_features();
    Chunk_t decompress_on_demand(uint64_t chunk_id);

public:
    TreeCache2();
    ~TreeCache2();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, uint64_t HitSF, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsHit(uint64_t FatherID, uint64_t HitID, SuperFeatures sfs);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
};
#endif