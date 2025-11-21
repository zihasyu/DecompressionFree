#ifndef SUBTREE_PRE_H
#define SUBTREE_RRE_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../cache.hpp"
#include "../topK_cache_policy.hpp"

using namespace std;

class SubTreePre : public AbsMethod
{
private:
    string myName_ = "SubTreePre";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    caches::fixed_sized_cache<
        uint64_t,           // Key 类型，例如 chunkID
        std::vector<uint8_t>,            // Value 类型
        caches::TopKCachePolicy // 缓存策略
    > chunkCache;

    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;

public:
    SubTreePre();
    ~SubTreePre();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, uint64_t HitSF);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsHit(uint64_t HitID);
    void SubTree(uint64_t HitSF, uint64_t HitFirstLayerID);
};
#endif