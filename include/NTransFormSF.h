#ifndef NTRANSFORMSF_H
#define NTRANSFORMSF_H

#include <bits/stdc++.h>
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <tuple>
#include <climits>
#include <map>
#include <vector>
#include <algorithm>
#include <functional>
#include <random>
#include <xxhash.h>

using namespace std;

#define SYSTEM_CIPHER_SIZE 32
#define FEATURE_NUMBER 12
#define FEATURE_NUMBER_PER_SF 4
#define SF_NUMBER FEATURE_NUMBER / FEATURE_NUMBER_PER_SF
// int featureNumber = SF_NUMBER;

class FeatureGen
{
private:
    /*VarSize chunking*/
    /*sliding window size*/
    uint64_t slidingWinSize_ = 64;
    uint64_t polyBase_;
    /*the modulus for limiting the value of the polynomial in rolling hash*/
    uint64_t polyMOD_;
    /*note: to avoid overflow, _polyMOD*255 should be in the range of "uint64_t"*/
    /*      here, 255 is the max value of "unsigned char"                       */
    /*the lookup table for accelerating the power calculation in rolling hash*/
    uint64_t *powerLUT_;
    /*the lookup table for accelerating the byte remove in rolling hash*/
    uint64_t *removeLUT_;
    uint32_t *feature;
    uint64_t *superfeature;
    map<uint64_t, std::vector<int>> *sfTable;
    std::mt19937 gen1, gen2;
    std::uniform_int_distribution<uint32_t> full_uint32_t;
    // uint64_t mSet[20] = {606610977102444280, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511, 3517, 3527, 3529, 3533, 3539, 3541, 3547, 3557, 3559, 3571};
    // uint64_t aSet[20] = {3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3181, 3187, 3191, 3203, 3209, 3217, 3221, 3229, 3251, 3253, 3257};
    uint64_t mSet[12];
    uint64_t aSet[12];

public:
    FeatureGen();
    ~FeatureGen();
    int getFeatureList(u_char *chunkBuffer, int chunkSize, vector<uint64_t> &featureList, vector<uint64_t> &sf);
    void insert(int label);
    void insertSFIndex(vector<uint64_t> &sf, uint64_t id);
    int querySF(const vector<uint64_t> &sf);
};

#endif