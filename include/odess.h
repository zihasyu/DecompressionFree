#ifndef ODESS_H
#define ODESS_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

using namespace std;

class Odess : public AbsMethod
{
private:
    string myName_ = "Odess";
    int PrevDedupChunkid = -1;
    int Version = 0;

public:
    Odess();
    ~Odess();
    void ProcessTrace();
};
#endif