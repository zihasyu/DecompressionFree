#pragma once
#include "lruCache.h"
#include <array>
#include <vector>
#include <cstring>
#include <cstdint>

template <size_t MaxChunkSize, size_t PoolCapacity>
class ChunkBufferPool
{
public:
    struct CachedEntry
    {
        size_t bufferIndex;
        size_t dataSize;

        CachedEntry() : bufferIndex(0), dataSize(0) {}
        CachedEntry(size_t idx, size_t size) : bufferIndex(idx), dataSize(size) {}
    };

private:
    std::array<std::array<uint8_t, MaxChunkSize>, PoolCapacity> buffers_;
    std::vector<size_t> freeList_;
    lru11::Cache<uint64_t, CachedEntry> lruCache_;

public:
    ChunkBufferPool()
        : lruCache_(PoolCapacity, 0)
    {
        freeList_.reserve(PoolCapacity);
        for (size_t i = 0; i < PoolCapacity; ++i)
        {
            freeList_.push_back(PoolCapacity - 1 - i);
        }
    }

    // 尝试获取，返回指向 buffer 的指针（零拷贝读取）
    uint8_t *tryGet(uint64_t chunkId, size_t &outSize)
    {
        CachedEntry entry;
        if (!lruCache_.tryGet(chunkId, entry))
        {
            return nullptr;
        }
        outSize = entry.dataSize;
        return buffers_[entry.bufferIndex].data();
    }

    // 插入数据到缓存
    void insert(uint64_t chunkId, const uint8_t *data, size_t size)
    {
        CachedEntry existingEntry;
        if (lruCache_.tryGet(chunkId, existingEntry))
        {
            memcpy(buffers_[existingEntry.bufferIndex].data(), data, size);
            existingEntry.dataSize = size;
            lruCache_.insert(chunkId, existingEntry);
            return;
        }

        size_t bufferIndex;
        if (!freeList_.empty())
        {
            bufferIndex = freeList_.back();
            freeList_.pop_back();
        }
        else
        {
            CachedEntry victimEntry = lruCache_.pruneValue();
            bufferIndex = victimEntry.bufferIndex;
        }

        memcpy(buffers_[bufferIndex].data(), data, size);
        lruCache_.insert(chunkId, CachedEntry(bufferIndex, size));
    }

    bool remove(uint64_t chunkId)
    {
        CachedEntry entry;
        if (lruCache_.tryGet(chunkId, entry))
        {
            freeList_.push_back(entry.bufferIndex);
            return lruCache_.remove(chunkId);
        }
        return false;
    }

    bool contains(uint64_t chunkId) const
    {
        return lruCache_.contains(chunkId);
    }

    size_t size() const { return lruCache_.size(); }
    size_t capacity() const { return PoolCapacity; }

    void clear()
    {
        lruCache_.clear();
        freeList_.clear();
        for (size_t i = 0; i < PoolCapacity; ++i)
        {
            freeList_.push_back(PoolCapacity - 1 - i);
        }
    }
};