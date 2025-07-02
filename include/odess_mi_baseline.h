#ifndef ODESS_MI_BL_H
#define ODESS_MI_BL_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class OdessMiBL : public AbsMethod
{
private:
    string myName_ = "OdessMiBL";
    int PrevDedupChunkid = -1;
    int Version = 0;

public:
    OdessMiBL();
    ~OdessMiBL();
    void ProcessTrace();
};
#endif