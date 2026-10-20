#ifndef DESIGN3_A_H
#define DESIGN3_A_H

#include "design3.h"

// Keeps Design3's group processing and selective expansion, while fixing
// every search to the nearest lossless ancestor in the rootChunkMap hierarchy.
class Design3_A : public Design3
{
public:
    Design3_A() : Design3(false, true) {}
};

#endif
