#ifndef OFFLINE_TREE_CUT_LAYER_IGNORE_H
#define OFFLINE_TREE_CUT_LAYER_IGNORE_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"

using namespace std;

class OfflineTreeCutLayerIgnore : public AbsMethod
{
private:
    string myName_ = "OfflineTreeCutLayerIgnore";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

public:
    OfflineTreeCutLayerIgnore();
    ~OfflineTreeCutLayerIgnore();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsHit(uint64_t FatherID, uint64_t HitID, SuperFeatures sfs);
};
#endif