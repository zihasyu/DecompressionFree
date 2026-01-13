#ifndef ALL_GREEDY_LRU_H
#define ALL_GREEDY_LRU_H

#include "absmethod.h"
#include "odess_similarity_detection.h"
#include "cache.hpp"
#include "cache_policy.hpp"

using namespace std;

class AllGreedyLRU : public AbsMethod
{
private:
    string myName_ = "AllGreedyLRU";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    // LRU cache及统计
    caches::fixed_sized_cache<uint64_t, std::vector<uint8_t>, caches::LRUCachePolicy> chunkCache{1024};
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;

public:
    AllGreedyLRU();
    ~AllGreedyLRU();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);

    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
};
#endif