#ifndef SUBTREE_REDUCTION_H
#define SUBTREE_REDUCTION_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class SubTreeReduction : public AbsMethod
{
private:
    string myName_ = "SubTreeReduction";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    SubTreeReduction();
    ~SubTreeReduction();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, uint64_t HitSF);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsHit(uint64_t HitID);
    void SubTree(uint64_t HitSF, uint64_t HitFirstLayerID);
};
#endif