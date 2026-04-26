#ifndef DESIGN_5_H
#define DESIGN_5_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../chunkbufferpool.h"
#include "../lruCache.h"
// #include "cache.hpp"
// #include "cache_policy.hpp"
#include <unordered_map>
#include <memory>
#include <vector>
using namespace std;

class Design5 : public AbsMethod
{
private:
    string myName_ = "Design5";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    // lru11::Cache<uint64_t, std::vector<uint8_t>> chunkCache;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;
    std::unordered_map<uint64_t, uint64_t> logicalRootMap;
    struct LayerGainStats
    {
        double totalGain = 0.0;
        uint64_t sampleCount = 0;
    };
    std::unordered_map<uint64_t, std::vector<LayerGainStats>> layerGainHistory;
    size_t currentBackupOrdinal = 0;

    // vector<uint8_t> cacheData1, cacheData2, cacheData3, cacheData4, cacheData5;
    uint8_t *bro_basechunk_ptr_cache = nullptr;
    uint8_t *chi_basechunk_ptr_cache = nullptr;
    uint8_t *basechunk_ptr_cache = nullptr;
    ChunkBufferPool<MAX_CHUNK_SIZE, 1024> chunkCache_;

    bool ShouldStopAfterLayerGain(uint64_t treeKey, size_t layerIndex, double gain);

public:
    Design5();
    ~Design5();
    void ProcessTrace() override;
    void PrintOffline(double time, CommandLine_t CmdLine) override;
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    Chunk_t CutGreedy(uint64_t treeKey, uint64_t BasechunkId, const Chunk_t Targetchunk);
    void StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID);
};
#endif
