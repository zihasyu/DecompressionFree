#include "../../include/readCache.h"
#include "../../include/define.h"

ReadCache::ReadCache()
{
    this->readCache_ = new lru11::Cache<string, uint32_t>(CONTAINER_CACHE_SIZE, 0);
    cacheSize_ = CONTAINER_CACHE_SIZE;
    containerPool_ = (uint8_t **)malloc(cacheSize_ * sizeof(uint8_t *));
    for (size_t i = 0; i < cacheSize_; i++)
    {
        containerPool_[i] = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    }
    currentIndex_ = 0;
}

ReadCache::~ReadCache()
{
    for (size_t i = 0; i < cacheSize_; i++)
    {
        free(containerPool_[i]);
    }
    free(containerPool_);
    delete readCache_;
}

void ReadCache::InsertToCache(string &name, uint8_t *data, uint32_t length)
{
    if (readCache_->size() + 1 > cacheSize_)
    {
        // evict a item
        uint32_t replaceIndex = readCache_->pruneValue();
        // fprintf(stdout, "Cache evict: %u\n", replaceIndex);
        memcpy(containerPool_[replaceIndex], data, length);
        readCache_->insert(name, replaceIndex);
    }
    else
    {
        // directly using current index
        // fprintf(stdout, "Cache use: %lu\n", currentIndex_);
        memcpy(containerPool_[currentIndex_], data, length);
        readCache_->insert(name, currentIndex_);
        currentIndex_++;
    }
}

bool ReadCache::ExistsInCache(string &name)
{
    bool flag = false;
    flag = this->readCache_->contains(name);
    return flag;
}

uint8_t *ReadCache::ReadFromCache(string &name)
{
    uint32_t index = this->readCache_->get(name);
    return containerPool_[index];
}