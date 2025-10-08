#ifndef TREE_COLUMN_CACHE_LIMIT_H
#define TREE_COLUMN_CACHE_LIMIT_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class TreeColumnCacheLimit : public AbsMethod
{
private:
    string myName_ = "TreeColumnCacheLimit";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    std::queue<uint64_t> cache_queue;
    const size_t MAX_CACHE_SIZE = 1024;

public:
    TreeColumnCacheLimit();
    ~TreeColumnCacheLimit();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs);
};
#endif