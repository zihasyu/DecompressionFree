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
    int FinalVersion = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;
    // insight2
    std::unordered_map<size_t, uint64_t> reversePosCount_;
    std::vector<size_t> reversePosList_;
    // For Insight 12: 命中候选项中“最旧”条目的位置
    std::map<size_t, uint64_t> forwardPosCount_;
    std::vector<size_t> forwardPosList_;
    // insight3
    uint64_t hit_consistency_numerator_ = 0;
    uint64_t hit_consistency_denominator_ = 0;
    // For Insight 11: 统计与候选列表中最后一个块的 chunkID 距离
    std::vector<int64_t> distanceList_;
    void DumpDistanceStats(const std::string &path);

public:
    AllGreedy(int FinalVersion_);
    ~AllGreedy();
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t FindBest(SuperFeatures SF, const Chunk_t &Targetchunk);
    void DumpReversePosStats(const std::string &path);
    void DumpForwardPosStats(const std::string &path);
    // 新增：用于分析 SFindex 的统计函数
    void DumpSFIndexStats();
};
#endif