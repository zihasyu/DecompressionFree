#include "../../include/Tree/SequentialCache.h"
#include <filesystem>
#include <iostream>
#include <fstream>

SequentialSlidingCache::SequentialSlidingCache(const std::string &cacheFile)
    : diskCacheFile(cacheFile), currentDiskOffset(0), currentAccessIndex(0),
      hitCount(0), accessCount(0), diskIOCount(0)
{
    initializeDiskFile();
}

SequentialSlidingCache::~SequentialSlidingCache()
{
    clearCache();
}

void SequentialSlidingCache::insertRootChunk(uint64_t chunkID, const std::vector<uint8_t> &chunkData)
{
    // 添加到内存队列
    memoryQueue.push_back({chunkID, chunkData});

    // 创建索引条目
    CachedChunkInfo info;
    info.chunkID = chunkID;
    info.dataSize = chunkData.size();
    info.inMemory = true;
    info.diskOffset = 0; // 暂时不用

    size_t index = chunkIndex.size();
    chunkIndex.push_back(info);
    chunkIDToIndex[chunkID] = index;

    // 检查是否需要刷新到外存
    checkAndFlushToDisk();

    // std::cout << "Inserted root chunk " << chunkID << ", total cached: "
    //           << chunkIndex.size() << ", in memory: " << memoryQueue.size() << std::endl;
}

bool SequentialSlidingCache::tryGetSequential(uint64_t chunkID, std::vector<uint8_t> &data)
{
    accessCount++;

    auto it = chunkIDToIndex.find(chunkID);
    if (it == chunkIDToIndex.end())
    {
        return false; // 不在缓存中
    }

    size_t index = it->second;
    CachedChunkInfo &info = chunkIndex[index];

    if (info.inMemory)
    {
        // 在内存中查找
        for (const auto &pair : memoryQueue)
        {
            if (pair.first == chunkID)
            {
                data = pair.second;
                hitCount++;

                // 处理顺序访问
                handleSequentialAccess(index);
                return true;
            }
        }
    }
    else
    {
        // 从外存加载
        if (loadFromDisk(index, data))
        {
            hitCount++;
            diskIOCount++;

            // 处理顺序访问
            handleSequentialAccess(index);
            return true;
        }
    }

    return false;
}

void SequentialSlidingCache::checkAndFlushToDisk()
{
    if (memoryQueue.size() <= MEMORY_THRESHOLD)
    {
        return; // 还没到阈值
    }

    // 计算需要移出的数量（保持阈值以下）
    size_t toFlushCount = memoryQueue.size() - MEMORY_THRESHOLD + MEMORY_THRESHOLD / 4; // 多移出一些，避免频繁操作

    std::cout << "Flushing " << toFlushCount << " chunks to disk..." << std::endl;

    for (size_t i = 0; i < toFlushCount && !memoryQueue.empty(); ++i)
    {
        auto &front = memoryQueue.front();
        uint64_t chunkID = front.first;
        const auto &chunkData = front.second;

        // 写入外存
        size_t dataSize = chunkData.size();
        diskWriter.write(reinterpret_cast<const char *>(&dataSize), sizeof(dataSize)); // 先写大小
        diskWriter.write(reinterpret_cast<const char *>(chunkData.data()), dataSize);  // 再写数据
        diskWriter.flush();

        // 更新索引
        auto indexIt = chunkIDToIndex.find(chunkID);
        if (indexIt != chunkIDToIndex.end())
        {
            CachedChunkInfo &info = chunkIndex[indexIt->second];
            info.inMemory = false;
            info.diskOffset = currentDiskOffset;

            currentDiskOffset += sizeof(dataSize) + dataSize;
        }

        memoryQueue.pop_front();
    }

    // std::cout << "Flush completed. Memory queue size: " << memoryQueue.size()
    //           << ", Disk offset: " << currentDiskOffset << std::endl;
}

bool SequentialSlidingCache::loadFromDisk(size_t index, std::vector<uint8_t> &data)
{
    if (index >= chunkIndex.size())
    {
        return false;
    }

    const CachedChunkInfo &info = chunkIndex[index];
    if (info.inMemory)
    {
        return false; // 应该从内存读取
    }

    // 打开文件读取
    std::ifstream reader(diskCacheFile, std::ios::binary);
    if (!reader.is_open())
    {
        return false;
    }

    // 定位到指定位置
    reader.seekg(info.diskOffset);

    // 先读取数据大小
    size_t dataSize;
    reader.read(reinterpret_cast<char *>(&dataSize), sizeof(dataSize));

    // 验证大小
    if (dataSize != info.dataSize)
    {
        reader.close();
        return false;
    }

    // 读取数据
    data.resize(dataSize);
    reader.read(reinterpret_cast<char *>(data.data()), dataSize);

    reader.close();
    return true;
}

void SequentialSlidingCache::handleSequentialAccess(size_t accessIndex)
{
    // 检查是否为顺序访问（访问位置比当前位置大）
    if (accessIndex > currentAccessIndex + LOOKAHEAD_SIZE)
    {
        // 触发滑窗：清理早期的数据
        size_t cleanupUntil = accessIndex - LOOKAHEAD_SIZE;

        // std::cout << "Sequential access detected. Cleaning up until index " << cleanupUntil << std::endl;

        // 清理内存中的早期数据
        while (!memoryQueue.empty())
        {
            uint64_t frontChunkID = memoryQueue.front().first;
            auto it = chunkIDToIndex.find(frontChunkID);
            if (it != chunkIDToIndex.end() && it->second < cleanupUntil)
            {
                memoryQueue.pop_front();
            }
            else
            {
                break;
            }
        }

        // 更新当前访问位置
        currentAccessIndex = accessIndex;
    }
}

void SequentialSlidingCache::clearCache()
{
    memoryQueue.clear();
    chunkIndex.clear();
    chunkIDToIndex.clear();

    if (diskWriter.is_open())
    {
        diskWriter.close();
    }

    // 删除外存文件
    if (std::filesystem::exists(diskCacheFile))
    {
        std::filesystem::remove(diskCacheFile);
    }

    currentDiskOffset = 0;
    currentAccessIndex = 0;
    hitCount = 0;
    accessCount = 0;
    diskIOCount = 0;

    std::cout << "Sequential cache cleared." << std::endl;
}

void SequentialSlidingCache::printStats() const
{
    float hitRate = accessCount > 0 ? (float)hitCount / accessCount * 100.0f : 0.0f;
    std::cout << "Sequential Cache Stats - Hits: " << hitCount
              << " Accesses: " << accessCount
              << " Hit Rate: " << hitRate << "%"
              << " Disk I/Os: " << diskIOCount
              << " Total Chunks: " << chunkIndex.size()
              << " In Memory: " << memoryQueue.size() << std::endl;
}

void SequentialSlidingCache::initializeDiskFile()
{
    // 确保目录存在
    std::filesystem::path filePath(diskCacheFile);
    if (filePath.has_parent_path())
    {
        std::filesystem::create_directories(filePath.parent_path());
    }

    // 打开文件用于写入
    diskWriter.open(diskCacheFile, std::ios::binary | std::ios::trunc);
    if (!diskWriter.is_open())
    {
        std::cerr << "Failed to open disk cache file: " << diskCacheFile << std::endl;
    }
}