#ifndef ALL_GREEDY_LFU_H
#define ALL_GREEDY_LFU_H

#include "absmethod.h"
#include "odess_similarity_detection.h"
#include "cache.hpp"
#include "cache_policy.hpp"

using namespace std;

class AllGreedyLFU : public AbsMethod
{
private:
    string myName_ = "AllGreedyLFU";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    //caches::fixed_sized_cache<uint64_t, std::vector<uint8_t>, caches::LFUCachePolicy> chunkCache;
    std::unique_ptr<caches::fixed_sized_cache<uint64_t, std::vector<uint8_t>, caches::LFUCachePolicy>> chunkCache;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;

public:
    AllGreedyLFU();
    ~AllGreedyLFU();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);

    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
};
#endif