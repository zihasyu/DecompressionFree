#ifndef PALANTIR_H
#define PALANTIR_H

#include "absmethod.h"
#include "odess_similarity_detection.h"

typedef uint64_t feature_t;
typedef unsigned long long super_feature_t;
typedef vector<super_feature_t> SuperFeatures;

using namespace std;

typedef struct
{
    uint64_t chunkid;
    int version;
} chunkid_version;

class Palantir : public AbsMethod
{
private:
    string myName_ = "Palantir";
    int PrevDedupChunkid = -1;
    int SFDelete = 0;
    int SFNew = 0;

public:
    int Version = 0;
    Palantir();
    ~Palantir();
    void ProcessTrace();
    double LZ4Ratio = 2;
    unordered_map<super_feature_t, vector<uint64_t>> SFindex1;
    unordered_map<super_feature_t, vector<chunkid_version>> SFindex2;
    unordered_map<super_feature_t, vector<chunkid_version>> SFindex3;
    uint64_t SF_Find(const SuperFeatures &superfeatures);
    void SF_Insert(const SuperFeatures &superfeatures, const uint64_t chunkid);
    void CleanIndex();
    void Version_log(double time);
    void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio);
    void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double chunktime);
};
#endif