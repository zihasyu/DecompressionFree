#ifndef TREE_CACHE2_H
#define TREE_CACHE2_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../lruCache.h"
#include "SequentialCache.h"
#include <unordered_map>

using namespace std;

class TreeCache2 : public AbsMethod
{
private:
    string myName_ = "TreeCache2";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    // 原有的LRU缓存
    lru11::Cache<uint64_t, std::vector<uint8_t>, std::mutex> chunkCache;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    int hotThreshold = 2;
    std::unordered_map<uint64_t, int> chunkHotMap;
    // 双顺序缓存系统
    SequentialSlidingCache *currentVersionCache;  // 为下个version收集根节点
    SequentialSlidingCache *previousVersionCache; // 当前version使用的缓存
    size_t seqCacheHitCount = 0;
    size_t seqCacheAccessCount = 0;

public:
    TreeCache2();
    ~TreeCache2();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);

    // 新方法
    void switchVersionCache();
    bool tryGetFromSequentialCache(uint64_t chunkID, std::vector<uint8_t> &data);
};
#endif