#ifndef ABS_METHOD_H
#define ABS_METHOD_H

#include <string>
#include <chrono>
#include "define.h"
#include "chunker.h"
#include "lz4.h"
#include "datawrite.h"

extern "C"
{
#include "./config.h"
#include "./xdelta3.h"
}

using namespace std;

class AbsMethod
{
protected:
public:
    int ads_Version = 0;
    // util
    string filename;
    dataWrite *dataWrite_;
    uint8_t *lz4ChunkBuffer;
    uint8_t *hashBuf;
    uint8_t *deltaMaxChunkBuffer;
    EVP_MD_CTX *mdCtx;
    // statics
    uint64_t totalLogicalSize = 0;
    uint64_t totalCompressedSize = 0;
    uint64_t logicalchunkNum = 0;
    uint64_t uniquechunkNum = 0;
    uint64_t basechunkNum = 0;
    uint64_t deltachunkNum = 0;
    uint64_t bugCount = 0;
    uint64_t finessehit = 0;
    double DCESum = 0;
    uint64_t SFnum = 0;
    // SF time statics
    std::chrono::time_point<std::chrono::high_resolution_clock> startSF, endSF;
    std::chrono::duration<double> preSFTime;
    std::chrono::duration<double> SFTime;
    // time breakdown

    std::chrono::time_point<std::chrono::high_resolution_clock> startDedup, endDedup;
    std::chrono::duration<double> DedupTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startLocalityMatch, endLocalityMatch;
    std::chrono::duration<double> LocalityMatchTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startLocalityDelta, endLocalityDelta;
    std::chrono::duration<double> LocalityDeltaTime, LocalityDeltaTmp;
    std::chrono::time_point<std::chrono::high_resolution_clock> startLz4, endLz4;
    std::chrono::duration<double> lz4CompressionTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startFeatureMatch, endFeatureMatch;
    std::chrono::duration<double> FeatureMatchTime, FeatureMatchTime1;
    std::chrono::time_point<std::chrono::high_resolution_clock> startFeatureDelta, endFeatureDelta;
    std::chrono::duration<double> FeatureDeltaTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startIOGet, endIOGet;
    std::chrono::duration<double> IOGetTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startIOWrite, endIOWrite;
    std::chrono::duration<double> IOWriteTime;
    std::chrono::duration<double> deltaCompressionTime;

    // index
    unordered_map<string, int> FPindex; //(fp,chunkid)
    // 消息队列
    MessageQueue<Chunk_t> *recieveQueue;
    // MessageQueue<uint64_t> *MaskRecieveQueue;
    //  MessageQueue<Chunk_t> *outputMQ_; // to datawrite but not used
    unordered_map<string, vector<int>> *SFindex;
    std::chrono::duration<double> getSFTime;
    uint64_t computeSFtimes = 0;
    // total

    uint64_t preLogicalchunkiSize = 0;
    uint64_t logicalchunkSize = 0;
    uint64_t preuniquechunkSize = 0;
    uint64_t uniquechunkSize = 0;
    uint64_t dedupchunkSize = 0;
    uint64_t basechunkSize = 0;
    uint64_t basechunkOriSize = 0;
    uint64_t deltachunkSize = 0;
    uint64_t deltachunkOriSize = 0;
    uint64_t finessechunkSize = 0;
    uint64_t finessePrechunkSize = 0;
    uint64_t localchunkSize = 0;
    uint64_t localPrechunkSize = 0;

    uint64_t ContainerNum = 0;
    uint64_t ContainerSize = 0;
    // impact reduct
    uint64_t DedupReduct = 0;
    // DedupReduct+=tmpChunk.chunkSize;
    uint64_t DeltaReduct = 0;
    // DeltaReduct+=tmpChunk.chunkSize-tmpChunk.saveSize;
    uint64_t LocalReduct = 0;
    // LocalReduct+=tmpChunk.chunkSize-tmpChunk.saveSize;
    uint64_t LocalityReduct = 0;
    uint64_t FeatureReduct = 0;

    // evaluation for false filter
    bool IsFalseFilter = true;
    double AcceptThreshold = 0;
    // evaluation for Metadata-guided
    bool TurnOnNameHash = true;
    long sameCount = 0;
    long OnlyFeature = 0;
    long OnlyMeta = 0;
    long differentCount = 0;
    AbsMethod();
    ~AbsMethod();
    void SetFilename(string name);
    virtual void ProcessTrace() = 0;
    void SetInputMQ(MessageQueue<Chunk_t> *mq) { recieveQueue = mq; }
    // void SetInputMaskMQ(MessageQueue<uint64_t> *mq) { MaskRecieveQueue = mq; }
    //  void SetOutputMQ(MessageQueue<Chunk_t> *outputMQ)
    //  {
    //      outputMQ_ = outputMQ;
    //      return;
    //  }
    static bool compareNat(const std::string &a, const std::string &b);
    void GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash);
    int FP_Find(string fp);
    bool FP_Insert(string fp, int chunkid);
    void GetSF(unsigned char *ptr, EVP_MD_CTX *mdCtx, uint8_t *SF, int dataSize);
    int SF_Find(const char *key, size_t keySize);
    bool SF_Insert(const char *key, size_t keySize, int chunkid);
    uint8_t *xd3_encode(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    virtual void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double AcceptThreshold, bool IsFalseFilter);
    virtual void PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double chunktime, double AcceptThreshold, bool IsFalseFilter);
    virtual void PrintChunkInfo(int64_t time, CommandLine_t CmdLine);
    virtual void PrintChunkInfo(int64_t time, CommandLine_t CmdLine, double chunktime);

    void StatsDelta(Chunk_t &tmpChunk);
    void StatsDeltaFeature(Chunk_t &tmpChunk);
    void StatsDeltaLocality(Chunk_t &tmpChunk);
    virtual void Version_log(double time);
    virtual void Version_log(double time, double chunktime);
    void SetTime(std::chrono::time_point<std::chrono::high_resolution_clock> &atime);
};
#endif