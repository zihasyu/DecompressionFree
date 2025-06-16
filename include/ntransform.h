#ifndef NTRANSFORM_H
#define NTRANSFORM_H

#include "absmethod.h"
#include "NTransFormSF.h"

using namespace std;

class NTransForm : public AbsMethod
{
private:
  string myName_ = "NTransForm";
  int PrevDedupChunkid = -1;
  int Version = 0;

public:
  FeatureGen ntrans;
  vector<uint64_t> fealist;

  NTransForm();
  ~NTransForm();
  void ProcessTrace();
};
#endif