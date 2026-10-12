#ifndef DESIGN_3_RANDOM_H
#define DESIGN_3_RANDOM_H

#include "design3.h"

// Same offline pipeline as Design3, but selects bases by uniformly sampling
// the currently reachable tree candidates under the recorded Design3 budget.
class Design3_Random : public Design3
{
public:
    Design3_Random() : Design3(true) {}
};

#endif
