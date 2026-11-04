#ifndef ODESS_H
#define ODESS_H

#include "absmethod.h"
#include "odess_similarity_detection.h"
#include <vector>

using namespace std;

class Odess : public AbsMethod
{
private:
    string myName_ = "Odess";
    int PrevDedupChunkid = -1;
    int Version = 0;
    dataWrite *offlineIndexSource_ = nullptr;
    uint64_t offlineIndexBoundary_ = 0;
    bool promoteNewBase_ = false;
    uint64_t rebaseReplacementCount_ = 0;
    uint64_t rebaseBytesAppended_ = 0;
    uint64_t rebaseStaleBytesRetained_ = 0;
    uint64_t maxRebaseChainDepth_ = 0;
    std::chrono::duration<double> rebaseAppendTime_{0};
    std::unordered_map<uint64_t, uint64_t> rebaseDepth_;
    std::unordered_map<uint64_t, uint64_t> rebaseSubtreeHeight_;

    bool IsLosslessGroupRoot(uint64_t rootID) const;

public:
    int offlineMethod = -1;
    Odess(int offlineMethod = -1, bool promoteNewBase = false);
    Odess();
    ~Odess();
    void ProcessTrace();
    void RefreshGroupIndexCounts(dataWrite *offlineSource, uint64_t onlineChunkBoundary);
    bool IsPromoteNewBaseBaseline() const { return promoteNewBase_; }
    void PrintRebaseStats(std::ostream &out, uint64_t backupNumber) const;
};
#endif
