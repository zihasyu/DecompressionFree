#ifndef ODESS_MI_DF_H
#define ODESS_MI_DF_H

#include "absmethod.h"

using namespace std;

class OdessMiDF : public AbsMethod
{
private:
    string myName_ = "OdessMiDF";
    int PrevDedupChunkid = -1;
    int Version = 0;

public:
    OdessMiDF();
    ~OdessMiDF();
    void ProcessTrace();
};
#endif