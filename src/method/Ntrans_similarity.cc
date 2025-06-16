#include "../../include/NTransFormSF.h"

// extern int featureNumber;

FeatureGen::FeatureGen()
{
    feature = new uint32_t[FEATURE_NUMBER];
    superfeature = new uint64_t[SF_NUMBER];
    gen2 = std::mt19937(314159);
    sfTable = new std::map<uint64_t, std::vector<int>>[SF_NUMBER];
    full_uint32_t = std::uniform_int_distribution<uint32_t>(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());
    polyBase_ = 257;       /*a prime larger than 255, the max value of "unsigned char"*/
    polyMOD_ = UINT64_MAX; /*polyMOD_ - 1 = 0x7fffff: use the last 23 bits of a polynomial as its hash*/
    /*initialize the lookup table for accelerating the power calculation in rolling hash*/
    powerLUT_ = (uint64_t *)malloc(sizeof(uint64_t) * slidingWinSize_);
    /*powerLUT_[i] = power(polyBase_, i) mod polyMOD_*/
    powerLUT_[0] = 1;
    for (int i = 1; i < slidingWinSize_; i++)
    {
        /*powerLUT_[i] = (powerLUT_[i-1] * polyBase_) mod polyMOD_*/
        powerLUT_[i] = (powerLUT_[i - 1] * polyBase_) & polyMOD_;
        // cout << powerLUT_[i] << endl;
    }
    /*initialize the lookup table for accelerating the byte remove in rolling hash*/
    removeLUT_ = (uint64_t *)malloc(sizeof(uint64_t) * 256); /*256 for unsigned char*/
    for (int i = 0; i < 256; i++)
    {
        /*removeLUT_[i] = (- i * powerLUT_[_slidingWinSize-1]) mod polyMOD_*/
        removeLUT_[i] = (i * powerLUT_[slidingWinSize_ - 1]) & polyMOD_;
        if (removeLUT_[i] != 0)
        {
            removeLUT_[i] = (polyMOD_ - removeLUT_[i] + 1) & polyMOD_;
        }
        /*note: % is a remainder (rather than modulus) operator*/
        /*      if a < 0,  -polyMOD_ < a % polyMOD_ <= 0       */
    }

    std::default_random_engine e(0);
    std::uniform_int_distribution<uint64_t> dis(0, UINT64_MAX);
    for (int i = 0; i < FEATURE_NUMBER; i++)
    {
        mSet[i] = dis(e);
        aSet[i] = dis(e);
    }
}

FeatureGen::~FeatureGen()
{
    delete[] feature;
    delete[] superfeature;
    delete[] sfTable;
    free(powerLUT_);
    free(removeLUT_);
}

int FeatureGen::getFeatureList(u_char *chunkBufferIn, int chunkSize, vector<uint64_t> &featureList, vector<uint64_t> &sf)
{
    featureList.clear();
    for (int i = 0; i < FEATURE_NUMBER; ++i)
        feature[i] = 0;
    for (int i = 0; i < SF_NUMBER; ++i)
        superfeature[i] = 0;
    int chunkBufferCnt = 0;
    uint64_t winFp = 0;
    /*full fill sliding window*/
    vector<uint64_t> fpList;
    u_char chunkBuffer[chunkSize];
    memcpy(chunkBuffer, chunkBufferIn, chunkSize);
    while (chunkBufferCnt < chunkSize)
    {
        if (chunkBufferCnt < slidingWinSize_)
        {
            winFp = winFp + (chunkBuffer[chunkBufferCnt] * powerLUT_[slidingWinSize_ - chunkBufferCnt - 1]) & polyMOD_;
            chunkBufferCnt++;
            continue;
        }
        winFp &= (polyMOD_);
        fpList.push_back(winFp);

        unsigned short int v = chunkBuffer[chunkBufferCnt - slidingWinSize_];                   // queue
        winFp = ((winFp + removeLUT_[v]) * polyBase_ + chunkBuffer[chunkBufferCnt]) & polyMOD_; // remove queue front and add queue tail
        chunkBufferCnt++;
    }
    uint64_t modNumber = UINT_MAX;
    for (auto i = 0; i < FEATURE_NUMBER; i++)
    {
        uint64_t maxPi = 0;
        uint64_t targetFp = 0;
        for (auto j = 0; j < fpList.size(); j++)
        {
            uint64_t Pi = (mSet[i] * fpList[j] + aSet[i]) & modNumber;
            if (maxPi < Pi)
            {
                maxPi = Pi;
                targetFp = fpList[j];
            }
        }
        feature[i] = targetFp;
        featureList.push_back(targetFp);
    }
    fpList.clear();

    for (int i = 0; i < SF_NUMBER; ++i)
    {
        uint64_t temp[FEATURE_NUMBER_PER_SF];
        for (int j = 0; j < FEATURE_NUMBER_PER_SF; ++j)
        {
            temp[j] = feature[j * SF_NUMBER + i];
        }
        // superfeature[i] = XXH64(temp, sizeof(uint64_t) * FEATURE_NUMBER_PER_SF, 0);
        sf[i] = XXH64(temp, sizeof(uint64_t) * FEATURE_NUMBER_PER_SF, 0);
    }

    // uint32_t r = full_uint32_t(gen2) % SF_NUMBER;

    // for (int i = 0; i < SF_NUMBER; ++i)
    // {
    //     int index = (r + i) % SF_NUMBER;

    //     if (sfTable[index].count(superfeature[index]))
    //     {

    //         return sfTable[index][superfeature[index]].back();
    //     }
    // }

    return -1;
}

int FeatureGen::querySF(const vector<uint64_t> &sf)
{
    for (int i = 0; i < SF_NUMBER; ++i)
    {
        if (sfTable[i].count(sf[i]))
        {
            return sfTable[i][sf[i]].back();
        }
    }
    return -1;
}

void FeatureGen::insertSFIndex(vector<uint64_t> &sf, uint64_t id)
{
    for (int i = 0; i < SF_NUMBER; ++i)
    {
        sfTable[i][sf[i]].push_back(id);
    }
}

void FeatureGen::insert(int label)
{
    for (int i = 0; i < SF_NUMBER; ++i)
    {
        sfTable[i][superfeature[i]].push_back(label);
    }
}
