#ifndef TREE_CUT_LAYER_H
#define TREE_CUT_LAYER_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class TreeCutLayer : public AbsMethod
{
private:
    string myName_ = "TreeCutLayer";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    TreeCutLayer();
    ~TreeCutLayer();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
};
#endif