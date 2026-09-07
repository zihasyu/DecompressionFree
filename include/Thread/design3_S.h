#ifndef DESIGN3_S_H
#define DESIGN3_S_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../chunkbufferpool.h"
#include "../lruCache.h"
#include <memory>
#include <unordered_map>

using namespace std;

class Design3_S : public AbsMethod
{
private:
    string myName_ = "Design3_S";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;
    std::unordered_map<uint64_t, uint64_t> logicalRootMap;

    uint8_t *bro_basechunk_ptr_cache = nullptr;
    uint8_t *chi_basechunk_ptr_cache = nullptr;
    uint8_t *basechunk_ptr_cache = nullptr;
    ChunkBufferPool<MAX_CHUNK_SIZE, 1024> chunkCache_;

public:
    Design3_S();
    ~Design3_S();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer,
                               size_t targetChunkbuffer_size,
                               const uint8_t *baseChunkBuffer,
                               size_t baseChunkBuffer_size,
                               size_t *deltaChunkBuffer_size,
                               uint8_t *tmpbuffer);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk);
};

#endif
