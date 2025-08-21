#ifndef TREE_CACHE_H
#define TREE_CACHE_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

#include "../lruCache.h"

#include <unordered_map>

using namespace std;

class TreeCache : public AbsMethod
{
private:
    string myName_ = "TreeCache";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    lru11::Cache<uint64_t, std::vector<uint8_t>, std::mutex> chunkCache;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;

public:
    TreeCache();
    ~TreeCache();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
};
#endif