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
    std::unordered_map<size_t, uint64_t> reversePosCount_;
    std::vector<size_t> reversePosList_;

public:
    AllGreedy();
    ~AllGreedy();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);
    void DumpReversePosStats(const std::string &path);
};
#endif