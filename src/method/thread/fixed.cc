#include "../../../include/Thread/fixed.h"
#include <cstdint>

// 线程安全队列
template <typename T>
class ThreadSafeQueue
{
public:
    void push(const T &value)
    {
        std::lock_guard<std::mutex> lk(m_);
        q_.push(value);
        cv_.notify_one();
    }
    bool pop(T &value)
    {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this]
                 { return !q_.empty() || finished_; });
        if (q_.empty())
            return false;
        value = std::move(q_.front());
        q_.pop();
        return true;
    }
    void set_finished()
    {
        std::lock_guard<std::mutex> lk(m_);
        finished_ = true;
        cv_.notify_all();
    }

private:
    std::queue<T> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool finished_ = false;
};

// 用于线程间传递的结构体
struct RestoredChunkFixed
{
    uint64_t rootId;
    uint64_t cid;
    Chunk_t tmpChunk;
    uint64_t initialRootId;
};

Fixed::Fixed()
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

Fixed::~Fixed()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
    if (rootChunkMap != nullptr)
    {
        delete rootChunkMap;
    }
}

void Fixed::ProcessTrace()
{
    using namespace std;
    using namespace std::chrono;

    ThreadSafeQueue<RestoredChunkFixed> chunkQueue;

    std::map<uint64_t, const std::vector<uint64_t> &> sortedRootChunkMap;
    for (const auto &pair : *rootChunkMap)
    {
        sortedRootChunkMap.insert(pair);
        logicalRootMap[pair.first] = pair.first;
    }

    // 恢复线程
    std::thread restoreThread([&]()
                              {
        std::set<uint64_t> processed;
        std::function<void(uint64_t)> process_tree = [&](uint64_t rootId) {
            auto it = sortedRootChunkMap.find(rootId);
            if (it == sortedRootChunkMap.end() || processed.count(rootId)) return;
            const std::vector<uint64_t>& chunkIds = it->second;
            if (chunkIds.empty()) return;

            processed.insert(rootId);

            std::vector<uint64_t> subRoots;

            struct QueueItem {
                std::vector<uint64_t>::const_iterator iter;
                std::vector<uint64_t>::const_iterator end;
                uint64_t initialRootId;
            };
            std::queue<QueueItem> bfsQueue;
            bfsQueue.push({chunkIds.begin(), chunkIds.end(), rootId});

            while (!bfsQueue.empty())
            {
                QueueItem item = bfsQueue.front();
                bfsQueue.pop();

                if (item.iter == item.end)
                    continue;

                uint64_t cid = *item.iter;
                auto nextIter = item.iter;
                ++nextIter;
                if (nextIter != item.end) {
                    bfsQueue.push({nextIter, item.end, item.initialRootId});
                }

                auto startRestoreChunk = high_resolution_clock::now();
                Chunk_t tmpChunk = dataWrite_->Get_Chunk_MetaInfo(cid);
                if (tmpChunk.basechunkID >= 0)
                {
                    Chunk_t tmpPreChunk = dataWrite_->Get_Chunk_Info(tmpChunk.basechunkID);
                    Chunk_t tmpDeltaChunk = dataWrite_->Get_Chunk_Info(cid);
                    uint64_t tmpSize = 0;
                    tmpChunk.chunkPtr = xd3_decode(tmpDeltaChunk.chunkPtr, tmpDeltaChunk.saveSize, tmpPreChunk.chunkPtr, tmpPreChunk.chunkSize, &tmpSize);
                    tmpChunk.loadFromDisk = true;
                    if (tmpPreChunk.loadFromDisk)
                        free(tmpPreChunk.chunkPtr);
                    if (tmpDeltaChunk.loadFromDisk)
                        free(tmpDeltaChunk.chunkPtr);
                }
                else
                {
                    Chunk_t rawChunk = dataWrite_->Get_Chunk_Info(cid);
                    tmpChunk.chunkPtr = (uint8_t *)malloc(tmpChunk.chunkSize);
                    memcpy(tmpChunk.chunkPtr, rawChunk.chunkPtr, tmpChunk.chunkSize);
                    tmpChunk.loadFromDisk = true;
                    if (rawChunk.loadFromDisk)
                        free(rawChunk.chunkPtr);
                }
                auto endRestoreChunk = high_resolution_clock::now();
                RestoreChunkTime += endRestoreChunk - startRestoreChunk;

                auto subRootIt = sortedRootChunkMap.find(cid);
                if (subRootIt != sortedRootChunkMap.end() && cid != subRootIt->second[0])
                {
                    subRoots.push_back(cid);
                }

                chunkQueue.push(RestoredChunkFixed{rootId, cid, tmpChunk, item.initialRootId});
            }

            for (uint64_t subRoot : subRoots) {
                process_tree(subRoot);
            }
        };

        for (const auto &pair : sortedRootChunkMap)
        {
            uint64_t rootId = pair.first;
            const std::vector<uint64_t> &chunkIds = pair.second;
            if (chunkIds.empty() || rootId != chunkIds[0])
                continue;
            process_tree(rootId);
        }
        chunkQueue.set_finished(); });

    // 处理线程
    std::thread processThread([&]()
                              {
        RestoredChunkFixed item;
        while (chunkQueue.pop(item))
        {
            uint64_t rootId = item.rootId;
            uint64_t cid = item.cid;
            uint64_t basechunkid = logicalRootMap[item.initialRootId];
            if (cid == rootId)
                basechunkid = -1;
            Chunk_t &tmpChunk = item.tmpChunk;

            if (basechunkid != -1)
            {
                auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk);
                uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

                if (RestoreBasechunk.loadFromDisk)
                    free(RestoreBasechunk.chunkPtr);

                if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
                {
                    int tmpChunkLz4CompressSize = LZ4_compress_fast((char *)tmpChunk.chunkPtr, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                    if (tmpChunkLz4CompressSize > 0)
                    {
                        tmpChunk.deltaFlag = NO_DELTA;
                        tmpChunk.saveSize = tmpChunkLz4CompressSize;
                    }
                    else
                    {
                        tmpChunk.deltaFlag = NO_LZ4;
                        tmpChunk.saveSize = tmpChunk.chunkSize;
                    }
                    tmpChunk.basechunkID = -1;

                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    free(deltachunk);

                    if (tmpChunk.deltaFlag == NO_LZ4)
                        offline_dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                }
                else
                {
                    tmpChunk.deltaFlag = DELTA;
                    tmpChunk.basechunkID = RestoreBasechunk.chunkID;

                    if (offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID < 0)
                    {
                        offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID = tmpChunk.chunkID;
                    }
                    else
                    {
                        int broID = offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID;
                        while (offline_dataWrite_->chunklist[broID].FirstBroID >= 0)
                            broID = offline_dataWrite_->chunklist[broID].FirstBroID;
                        offline_dataWrite_->chunklist[broID].FirstBroID = tmpChunk.chunkID;
                    }

                    memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                    StatsDelta(tmpChunk);
                    free(deltachunk);

                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                }
            }
            else
            {
                int tmpChunkLz4CompressSize = LZ4_compress_fast((char *)tmpChunk.chunkPtr, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                if (tmpChunkLz4CompressSize > 0)
                {
                    tmpChunk.deltaFlag = NO_DELTA;
                    tmpChunk.saveSize = tmpChunkLz4CompressSize;
                }
                else
                {
                    tmpChunk.deltaFlag = NO_LZ4;
                    tmpChunk.saveSize = tmpChunk.chunkSize;
                }
                tmpChunk.basechunkID = -1;

                basechunkNum++;
                basechunkSize += tmpChunk.saveSize;

                if (tmpChunk.deltaFlag == NO_LZ4)
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                else
                    offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
            }

            uniquechunkNum++;
            uniquechunkSize += tmpChunk.saveSize;
            logicalchunkNum++;
            logicalchunkSize += tmpChunk.chunkSize;
        } });

    restoreThread.join();
    processThread.join();

    cout << "lru cache hit rate: " << (double)cacheHitCount / (double)cacheAccessCount << " cacheHitCount " << cacheHitCount << " cacheAccessCount " << cacheAccessCount << endl;
    return;
}

uint8_t *Fixed::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
{
    SetTime(startMiEncode);
    size_t deltachunkSize;
    int ret = xd3_encode_memory(targetChunkbuffer, targetChunkbuffer_size, baseChunkBuffer, baseChunkBuffer_size, tmpbuffer, &deltachunkSize, CONTAINER_MAX_SIZE * 2, 0);
    if (ret != 0)
    {
        cout << "delta error" << endl;
        const char *errMsg = xd3_strerror(ret);
        cout << errMsg << endl;
    }
    if (deltachunkSize <= 0)
        *deltaChunkBuffer_size = INT_MAX;
    else
        *deltaChunkBuffer_size = deltachunkSize;
    memcpy(tmpDeltaBuffer, tmpbuffer, deltachunkSize);
    SetTime(endMiEncode);
    SetTime(startMiEncode, endMiEncode, EncodeTime);
    return tmpDeltaBuffer;
}

Chunk_t Fixed::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;

    cacheAccessCount++;
    size_t cachedSize = 0;
    Chunk_t basechunk = offline_dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    if (uint8_t *cachedPtr = chunkCache_.tryGet(BasechunkId, cachedSize); cachedPtr != nullptr)
    {
        cacheHitCount++;
        basechunk.chunkPtr = cachedPtr;
        basechunk.loadFromDisk = false;
    }
    else
    {
        if (basechunk.basechunkID < 0)
        {
            basechunk = offline_dataWrite_->Get_Chunk_Info(BasechunkId);
        }
        else
        {
            basechunk = xd3_recursive_restore_offline_time(BasechunkId);
        }
        if (basechunk.chunkPtr != nullptr)
        {
            chunkCache_.insert(BasechunkId, basechunk.chunkPtr, basechunk.chunkSize);
        }
    }

    if (basechunk.FirstChildID < 0)
        return basechunk;

    memcpy(CombinedBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    memcpy(MinBaseBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    resultchunk.chunkSize = basechunk.chunkSize;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = basechunk.chunkID;
    resultchunk.FirstChildID = basechunk.FirstChildID;

    xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk.chunkPtr, basechunk.chunkSize, &resultchunk.saveSize, deltaMaxChunkBuffer);

    if (basechunk.loadFromDisk)
        free(basechunk.chunkPtr);

    bool end = false;
    uint64_t tmpsaveSize = 0;
    while (!end && resultchunk.FirstChildID >= 0)
    {
        uint64_t tmpChildID = resultchunk.chunkID;

        uint64_t childId = resultchunk.FirstChildID;
        Chunk_t TmpChildChunk = offline_dataWrite_->Get_Chunk_MetaInfo(childId);
        uint8_t *basechunk_ptr = nullptr;
        cacheAccessCount++;
        bool NeedFreeChild = false;
        if (uint8_t *cachedPtr2 = chunkCache_.tryGet(childId, cachedSize); cachedPtr2 != nullptr)
        {
            cacheHitCount++;
            basechunk_ptr = cachedPtr2;
            basechunk_size = TmpChildChunk.chunkSize;
            TmpChildChunk.loadFromDisk = false;
        }
        else
        {
            TmpChildChunk = offline_dataWrite_->Get_Chunk_Info(childId);
            basechunk_ptr = xd3_decode(TmpChildChunk.chunkPtr, TmpChildChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
            NeedFreeChild = true;
            if (basechunk_ptr != nullptr)
            {
                chunkCache_.insert(childId, basechunk_ptr, basechunk_size);
            }
        }

        xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer);
        if (tmpsaveSize < resultchunk.saveSize)
        {
            resultchunk.saveSize = tmpsaveSize;
            resultchunk.chunkID = TmpChildChunk.chunkID;
            resultchunk.chunkSize = TmpChildChunk.chunkSize;
            resultchunk.FirstChildID = TmpChildChunk.FirstChildID;
            memcpy(MinBaseBuffer, basechunk_ptr, TmpChildChunk.chunkSize);
        }
        if (TmpChildChunk.loadFromDisk)
            free(TmpChildChunk.chunkPtr);
        if (NeedFreeChild)
            free(basechunk_ptr);

        Chunk_t TmpBroChunk = TmpChildChunk;
        while (TmpBroChunk.FirstBroID >= 0)
        {
            uint64_t broId = TmpBroChunk.FirstBroID;
            uint8_t *bro_basechunk_ptr = nullptr;
            bool NeedFreeBro = false;

            cacheAccessCount++;
            if (uint8_t *cachedPtr3 = chunkCache_.tryGet(broId, cachedSize); cachedPtr3 != nullptr)
            {
                cacheHitCount++;
                TmpBroChunk = offline_dataWrite_->Get_Chunk_MetaInfo(broId);
                bro_basechunk_ptr = cachedPtr3;
                basechunk_size = TmpBroChunk.chunkSize;
                TmpBroChunk.loadFromDisk = false;
            }
            else
            {
                TmpBroChunk = offline_dataWrite_->Get_Chunk_Info(broId);
                bro_basechunk_ptr = xd3_decode(TmpBroChunk.chunkPtr, TmpBroChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
                NeedFreeBro = true;
                if (bro_basechunk_ptr != nullptr)
                {
                    chunkCache_.insert(broId, bro_basechunk_ptr, basechunk_size);
                }
            }

            xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, bro_basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer);
            if (tmpsaveSize < resultchunk.saveSize)
            {
                resultchunk.saveSize = tmpsaveSize;
                resultchunk.chunkID = TmpBroChunk.chunkID;
                resultchunk.chunkSize = TmpBroChunk.chunkSize;
                resultchunk.FirstChildID = TmpBroChunk.FirstChildID;
                memcpy(MinBaseBuffer, bro_basechunk_ptr, TmpBroChunk.chunkSize);
            }
            if (TmpBroChunk.loadFromDisk)
                free(TmpBroChunk.chunkPtr);
            if (NeedFreeBro)
                free(bro_basechunk_ptr);
        }
        if (resultchunk.chunkID == tmpChildID)
        {
            end = true;
        }
        else
        {
            basechunk.chunkSize = resultchunk.chunkSize;
            memcpy(CombinedBuffer, resultchunk.chunkPtr, resultchunk.chunkSize);
        }
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    return resultchunk;
}
