#ifndef CHUNKER_H
#define CHUNKER_H

#include "define.h"
#include "struct.h"
#include "messageQueue.h"

using namespace std;

enum ChunkTypeNum
{
    FIXED_SIZE = 0,
    FASTCDC,
    GEARCDC,
    TAR,
    TAR_MultiHeader,
    MTAR,
    MTAROdess,
    MTARPalantir
};
const int BigChunkSize = CONTAINER_MAX_SIZE;
class Chunker
{
private:
    /* data */
    string myName_ = "Chunker";
    int chunkType;
    // chunk size settings for FastCDC
    // uint64_t avgChunkSize_;
    // uint64_t minChunkSize_;
    // uint64_t maxChunkSize_;
    uint64_t minChunkSize = 4096;
    uint64_t avgChunkSize = 8192;
    uint64_t maxChunkSize = 16384;
    uint64_t normalSize;
    uint32_t bits;
    uint32_t maskS;
    uint32_t maskL;
    uint64_t HeaderCp = 0;
    // fixed Size Chunking
    uint64_t FixedChunkSize;
    // uint64_t MultiHeaderSize;

    // IO stream
    ifstream inputFile;

    // buffer
    uint8_t *readFileBuffer; //*waitingForChunkingBuffer;
    uint8_t *chunkBuffer;
    uint8_t *headerBuffer;
    uint8_t *dataBuffer;

    // Chunker ID
    uint64_t chunkID = 0;

    // messageQueue
    MessageQueue<Chunk_t> *outputMQ_;
    // MessageQueue<uint64_t> *MaskoutputMQ_;

    bool NameExist = 0;
    bool IsLongNameChunk = 0;
    std::unordered_set<std::string> nameHashSet;
    char name[101];
    char path[101];
    char LongName[513];
    const uint64_t prime = 1099511628211;

    // 记录边界
    // std::vector<std::tuple<uint64_t, uint32_t, char>> boundaries_; // offset, size, type(H/D/B)
    // uint64_t current_offset_;
    // std::string input_file_path_;

public:
    vector<double> MTarTime;
    int a;
    Chunker(int chunkType_);
    ~Chunker();
    // util method
    void LoadChunkFile(string path);
    void ChunkerInit();
    void Chunking();

    void SetOutputMQ(MessageQueue<Chunk_t> *outputMQ)
    {
        outputMQ_ = outputMQ;
        return;
    }
    // void SetOutputMaskMQ(MessageQueue<uint64_t> *outputMQ)
    // {
    //     MaskoutputMQ_ = outputMQ;
    //     return;
    // }

    uint32_t GenerateFastCDCMask(uint32_t bits);
    inline uint32_t CompareLimit(uint32_t input, uint32_t lower, uint32_t upper);
    uint32_t CalNormalSize(const uint32_t min, const uint32_t av, const uint32_t max);
    inline uint32_t DivCeil(uint32_t a, uint32_t b);
    // Chunking Methods
    uint32_t CutPointFixSized(const uint8_t *src, const uint64_t len);
    uint64_t CutPointFastCDC(const uint8_t *src, const uint64_t len);
    uint32_t CutPointGear(const uint8_t *src, const uint64_t len);
    uint64_t CutPointTarFast(const uint8_t *src, const uint64_t len);
    uint64_t CutPointTarHeader(const uint8_t *src, const uint64_t len);
    void MTar(vector<string> &readfileList, uint32_t backupNum);
    // uint32_t CutPoint(const uint8_t *src, const uint32_t len); // TarSegment is going to use it
    std::chrono::time_point<std::chrono::high_resolution_clock> startChunk, endChunk;
    std::chrono::duration<double> ChunkTime;

    void SetTime(std::chrono::time_point<std::chrono::high_resolution_clock> &atime)
    {
        atime = std::chrono::high_resolution_clock::now();
    }
    int Next_Chunk_Type = FILE_HEADER;
    int localType = FILE_HEADER;
    size_t localOffset = 0;
    uint64_t Next_Chunk_Size = 0;
    uint64_t Big_Chunk_Allowance = 0; // CutPointTar
    uint64_t Big_Chunk_Last_Size = 0; // CutPointTar
    uint64_t Big_Chunk_Size = 0;      // CutPointTarFast
    uint64_t Big_Chunk_Offset = 0;    // CutPointTarFast
    long sameCount = 0;
    uint64_t recipeSegCount = 0;
    bool FindName(const char *src);
    bool FindLongName(const char *src);
    const char *FindNameBegin(const char *src);
    const char *FindLongNameBegin(const char *src);
    uint16_t hashNameToUint16(const char *name);
    uint64_t hashNameToUint64(const char *name);
    bool ExtractPath(const char *full);
    int MULTI_HEADER_CHUNK = 16;
    // void SetHeaderChunkSize(uint64_t size);

    // 记录边界
    // void WriteBoundariesToFile();
    // void ClearBoundaries() { boundaries_.clear(); }
};
#endif