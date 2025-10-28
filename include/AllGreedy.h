#ifndef ALL_GREEDY_H
#define ALL_GREEDY_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class AllGreedy : public AbsMethod
{
private:
    string myName_ = "AllGreedy";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    // std::queue<uint64_t> cache_queue;
    // size_t max_cache_size = 1024;
    // void updateCacheSize();

    void ResizeRowCache();

    uint64_t time_lz4_disk = 0;
    uint64_t time_lz4_mem = 0;
    uint64_t time_delta_disk = 0;
    uint64_t time_delta_mem = 0;

public:
    AllGreedy();
    ~AllGreedy();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);
};
#endif