#ifndef ODESS_MI_TREE_H
#define ODESS_MI_TREE_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class OdessMLTree : public AbsMethod
{
private:
    string myName_ = "OdessMiBL2";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    OdessMLTree();
    ~OdessMLTree();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    uint64_t find_best_basechunk(uint64_t BasechunkId, const Chunk_t& Targetchunk);
};
#endif