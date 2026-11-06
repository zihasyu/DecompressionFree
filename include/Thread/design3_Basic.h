#ifndef DESIGN3_BASIC_H
#define DESIGN3_BASIC_H

#include "design3_G.h"

// Basic ablation: same as Design3_GT, with adaptive search-start switching disabled.
class Design3_Basic : public Design3_G
{
public:
    Design3_Basic() : Design3_G(false, false) {}
};

#endif
