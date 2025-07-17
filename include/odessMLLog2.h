#ifndef ODESS_ML_LOG2_H
#define ODESS_ML_LOG2_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class OdessMLLog2 : public AbsMethod
{
private:
    string myName_ = "OdessMLLog2";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    OdessMLLog2();
    ~OdessMLLog2();
    void ProcessTrace();
    Chunk_t xd3_recursive_restore_time(uint64_t BasechunkId, const Chunk_t Targetchunk);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
};
#endif