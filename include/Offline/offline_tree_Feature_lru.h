#ifndef OFFLINE_TREE_FEATURE_LRU_H
#define OFFLINE_TREE_FEATURE_LRU_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

#include "../lruCache.h"
// #include "cache.hpp"
// #include "cache_policy.hpp"
#include <unordered_map>
#include <memory>
using namespace std;

class OfflineTreeFeatureLru : public AbsMethod
{
private:
    string myName_ = "OfflineTreeFeatureLru";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    lru11::Cache<uint64_t, std::vector<uint8_t>, std::mutex> chunkCache;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;
    std::unordered_map<uint64_t, uint64_t> logicalRootMap;

    vector<uint8_t> cacheData1, cacheData2, cacheData3, cacheData4, cacheData5;

public:
    OfflineTreeFeatureLru();
    ~OfflineTreeFeatureLru();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk);
    void StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID);
};
#endif