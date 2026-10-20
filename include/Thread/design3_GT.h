#ifndef DESIGN3_GT_H
#define DESIGN3_GT_H

#include "design3_G.h"

// Disables both group-based layout (inherited from Design3_G) and the
// depth-aware selective tree-expansion threshold.
class Design3_GT : public Design3_G
{
public:
    Design3_GT() : Design3_G(false) {}
};

#endif
