/*
 * @Author: Helix0503 834991203@qq.com
 * @Date: 2024-01-08 16:42:12
 * @LastEditors: Helix0503 834991203@qq.com
 * @LastEditTime: 2024-01-30 15:58:16
 * @FilePath: /Maplemeo/LocalDedupSim/include/dataWrite.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#ifndef DATA_WRITE_H
#define DATA_WRITE_H
#include "struct.h"
#include "define.h"
#include "messageQueue.h"
#include "readCache.h"
#include <vector>
#include <sstream>
#include <mutex>
#include "lz4.h"
extern "C"
{
#include "./xdelta3.h"
}
using namespace std;

// static int curContainerIdGlobal = 0;
static int containerNum = 0;
static Container_t curContainer;
static uint64_t curOffset = 0;
class dataWrite
{
private:
    int chunkNum = 0;
    int containerSize = 0;
    string filename;
    uint64_t loadContainerTimes = 0;
    uint64_t cacheHitTimes = 0;

    std::mutex mtx; // 互斥锁

    // unordered_map<string, int> FPindex; //(fp,chunkid)
    //  unordered_map<uint32_t, vector<int>> ObjectIndex;
    // unordered_map<string, vector<int>> ObjectIndex;
    vector<Chunk_t> recipelist;
    unordered_map<string, vector<Recipe_t>> RecipeMap;
    unordered_map<string, vector<Recipe_Header_t>> RecipeMap_header;
    // unordered_map<string, vector<int>> *SFindex;
    //  static Container_t curContainer;
    ReadCache *containerCache;
    std::chrono::duration<double> readIOTime;
    std::chrono::duration<double> writeIOTime;
    std::chrono::duration<double> UpdateCacheTime;
    std::chrono::duration<double> readCacheTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime, endTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime2, endTime2;

    int Next_Chunk_Type = FILE_HEADER;
    int localType = FILE_HEADER;
    uint8_t *MultiHeaderBuffer;
    uint64_t MultiHeaderChunkSize = 0;
    uint64_t MultiHeaderOffset = 0;
    size_t localOffset = 0;
    uint64_t Next_Chunk_Size = 0;
    uint64_t Big_Chunk_Allowance = 0; // CutPointTar
    uint64_t Big_Chunk_Last_Size = 0; // CutPointTar
    uint64_t Big_Chunk_Size = 0;      // CutPointTarFast
    uint64_t Big_Chunk_Offset = 0;    // CutPointTarFast
    uint32_t minChunkSize = 4096;
    uint32_t avgChunkSize = 8192;
    uint32_t maxChunkSize = 16384;
    uint32_t normalSize;
    uint32_t bits;
    uint32_t maskS;
    uint32_t maskL;
    uint8_t *lz4SafeChunkBuffer;

public:
    void SetFilename(string name);
    vector<Chunk_t> chunklist;
    void writing();
    MessageQueue<Chunk_t> *recieveQueue;
    void SetInputMQ(MessageQueue<Chunk_t> *mq)
    {
        if (mq == nullptr)
        {
            // 处理错误或返回
            std::cerr << "Error: mq is nullptr in SetInputMQ\n";
            return;
        }
        recieveQueue = mq;
    }
    MessageQueue<Container_t> *MQ;
    bool Chunk_Insert(Chunk_t chunk);
    bool Chunk_Insert(Chunk_t chunk, uint8_t *lz4Buffer);
    int Get_Chunk_Num();
    int Get_Container_Num(Chunk_t chunk);
    Chunk_t Get_Chunk_Info(int id);

    bool Recipe_Insert(uint64_t chunkID);
    bool Recipe_Header_Insert(uint64_t chunkID);
    void restoreFile(string fileName);
    void restoreHeaderFile(string fileName);
    void MTar2Tar(string fileName);

    void Save_to_File(string methodname);
    void Save_to_File_unique(string methodname);
    void Save_to_File_Chunking(string methodname);
    void writeContainers();
    void ProcessLastContainer();
    void PrintBinaryArray(const uint8_t *buffer, size_t buffer_size);
    bool isLz4(int id);
    bool isDuplicate(int id);
    uint8_t *xd3_decode(const uint8_t *in, size_t in_size, const uint8_t *ref, size_t ref_size, size_t *res_size);
    uint32_t CutPointTarFast(const uint8_t *src, const uint32_t len);
    uint32_t CutPointFastCDC(const uint8_t *src, const uint32_t len);
    uint32_t CalNormalSize(const uint32_t min, const uint32_t av, const uint32_t max);
    inline uint32_t DivCeil(uint32_t a, uint32_t b);
    uint32_t GenerateFastCDCMask(uint32_t bits);
    inline uint32_t CompareLimit(uint32_t input, uint32_t lower, uint32_t upper);

    Chunk_t Get_Chunk_MetaInfo(int id);
    void PrintMetrics();
    static void chunkprint(const Chunk_t chunk);
    dataWrite();
    ~dataWrite();
};

#endif