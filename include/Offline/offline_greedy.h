#ifndef OFFLINE_ALL_GREEDY_H
#define OFFLINE_ALL_GREEDY_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class OfflineAllGreedy : public AbsMethod
{
private:
    string myName_ = "OfflineAllGreedy";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    OfflineAllGreedy();
    ~OfflineAllGreedy();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);
};
#endif