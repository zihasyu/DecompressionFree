#ifndef ODESS_MULTI_LAYERED_H
#define ODESS_MULTI_LAYERED_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class Odess_Multi_Layered : public AbsMethod
{
private:
    string myName_ = "Odess_Multi_Layered";
    int PrevDedupChunkid = -1;
    int DedupGap = 0;
    int Version = 0;
    FeatureIndexTable table;

public:
    Odess_Multi_Layered();
    ~Odess_Multi_Layered();
    bool ProcessTrace(string inputFileName);
};
#endif