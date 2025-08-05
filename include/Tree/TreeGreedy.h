#ifndef TREE_GREEDY_H
#define TREE_GREEDY_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class TreeGreedy : public AbsMethod
{
private:
    string myName_ = "TreeGreedy";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    TreeGreedy();
    ~TreeGreedy();
    void ProcessTrace();
    Chunk_t xd3_recursive_restore(uint64_t BasechunkId, const Chunk_t Targetchunk);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
};
#endif