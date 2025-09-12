#ifndef SEQUENTIAL_CACHE_H
#define SEQUENTIAL_CACHE_H

#include <vector>
#include <unordered_map>
#include <deque>
#include <string>
#include <fstream>

struct CachedChunkInfo
{
    uint64_t chunkID;
    size_t dataSize;
    size_t diskOffset; // 在外存文件中的偏移
    bool inMemory;
};

class SequentialSlidingCache
{
private:
    static const size_t MEMORY_THRESHOLD = 1024 * 60; // 内存阈值
    static const size_t LOOKAHEAD_SIZE = 1;           // 预测窗口大小

    // 内存数据存储
    std::deque<std::pair<uint64_t, std::vector<uint8_t>>> memoryQueue;

    // 外存管理
    std::string diskCacheFile;
    std::ofstream diskWriter;
    size_t currentDiskOffset;

    // 索引管理
    std::vector<CachedChunkInfo> chunkIndex;
    std::unordered_map<uint64_t, size_t> chunkIDToIndex;

    // 访问位置跟踪
    size_t currentAccessIndex;

    // 统计信息
    size_t hitCount;
    size_t accessCount;
    size_t diskIOCount;

public:
    SequentialSlidingCache(const std::string &cacheFile = "./SeqCache.dat");
    ~SequentialSlidingCache();

    // 动态插入根节点
    void insertRootChunk(uint64_t chunkID, const std::vector<uint8_t> &chunkData);

    // 顺序访问接口
    bool tryGetSequential(uint64_t chunkID, std::vector<uint8_t> &data);

    // 清理接口
    void clearCache();

    // 统计信息
    void printStats() const;

private:
    // 检查是否需要将内存数据移到外存
    void checkAndFlushToDisk();

    // 从外存加载数据
    bool loadFromDisk(size_t index, std::vector<uint8_t> &data);

    // 滑窗处理
    void handleSequentialAccess(size_t accessIndex);

    // 初始化外存文件
    void initializeDiskFile();
};

#endif