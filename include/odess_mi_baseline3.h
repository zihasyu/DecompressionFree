#ifndef ODESS_MI_BL3_H
#define ODESS_MI_BL3_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class OdessMiBL3 : public AbsMethod
{
private:
    string myName_ = "OdessMiBL3";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    OdessMiBL3();
    ~OdessMiBL3();
    void ProcessTrace();
    Chunk_t xd3_recursive_restore_BL2_time(uint64_t BasechunkId, const Chunk_t Targetchunk);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
};
#endif