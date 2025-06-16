#ifndef LOCAL_METHOD_H
#define LOCAL_METHOD_H

#include "absmethod.h"

using namespace std;

class LocalDedup : public AbsMethod
{
private:
    const string myName_ = "Local Dedup";
    PLChunk plchunk;
    int PrevDedupChunkid = -1;
    int DedupGap = 0;
    int Version = 0;
    int localError = 0;
    bool localFlag = true;
    uint64_t localLogicalSize = 0;
    uint64_t localUniqueSize = 0;

public:
    LocalDedup();
    ~LocalDedup();
    void ProcessTrace();
    virtual void Version_log(double time);
    virtual void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double chunktime);
    virtual void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio);
};

#endif