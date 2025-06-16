#ifndef DEDUP_METHOD_H
#define DEDUP_METHOD_H

#include "absmethod.h"

using namespace std;

class Dedup : public AbsMethod
{
private:
    string myName_ = "Dedup";

public:
    Dedup();
    ~Dedup();
    void ProcessTrace();
};
#endif