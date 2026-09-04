#include "../../../include/Thread/design3_G.h"
#include <cmath>
#include <cstdint>

namespace
{
constexpr double kAdaptiveThresholdA = 64.0;

int ComputeChunkDepth(const std::vector<Chunk_t> &chunklist, int chunkId)
{
    int depth = 0;
    while (chunkId >= 0)
    {
        depth++;
        chunkId = chunklist[chunkId].basechunkID;
    }
    return depth;
}

double ComputeTreeInsertThreshold(const std::vector<Chunk_t> &chunklist, int baseChunkId)
{
    const int newNodeDepth = ComputeChunkDepth(chunklist, baseChunkId) + 1;
    if (newNodeDepth <= 1)
    {
        return 0.0;
    }
    return kAdaptiveThresholdA * std::log(static_cast<double>(newNodeDepth));
}
} // namespace

Design3_G::Design3_G()
// : chunkCache(1024) // 在构造函数初始化列表中初始化缓存容量
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    //     bro_basechunk_ptr_cache = (uint8_t *)malloc(MAX_CHUNK_SIZE * sizeof(uint8_t));
    //     chi_basechunk_ptr_cache = (uint8_t *)malloc(MAX_CHUNK_SIZE * sizeof(uint8_t));
    //     basechunk_ptr_cache = (uint8_t *)malloc(MAX_CHUNK_SIZE * sizeof(uint8_t));
}

Design3_G::~Design3_G()
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
struct RestoredChunk3G
{
    uint64_t rootId;
    uint64_t cid;
    Chunk_t tmpChunk;
    uint64_t initialRootId;
};

void Design3_G::ProcessTrace()
{
    using namespace std;
    using namespace std::chrono;

    ThreadSafeQueue<RestoredChunk3G> chunkQueue;

    std::map<uint64_t, const std::vector<uint64_t> &> sortedRootChunkMap;
    for (const auto &pair : *rootChunkMap)
    {
        sortedRootChunkMap.insert(pair);
        logicalRootMap[pair.first] = pair.first;
    }

    // 恢复线程（按逻辑顺序：chunkID 递增，而非按集合/树顺序）
    std::thread restoreThread([&]()
                              {
        // 构建 chunkID -> 所属根集合 rootId 的映射
        std::unordered_map<uint64_t, uint64_t> chunkToRoot;
        for (const auto &pair : sortedRootChunkMap)
        {
            for (uint64_t cid : pair.second)
            {
                if (chunkToRoot.find(cid) == chunkToRoot.end())
                    chunkToRoot[cid] = pair.first;
            }
        }

        // 按 chunkID 递增顺序逐个恢复（每个 chunk 只处理一次）
        const size_t totalChunkNum = dataWrite_->chunklist.size();
        for (uint64_t cid = 0; cid < totalChunkNum; cid++)
        {
            auto rootIt = chunkToRoot.find(cid);
            if (rootIt == chunkToRoot.end())
                continue; // 跳过不存在的 chunk（空洞）

            uint64_t rootId = rootIt->second;

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

            chunkQueue.push(RestoredChunk3G{rootId, cid, tmpChunk, rootId});
        }
        chunkQueue.set_finished(); });

    // 处理线程
    std::thread processThread([&]()
                              {
        RestoredChunk3G item;
        while (chunkQueue.pop(item))
        {
            uint64_t rootId = item.rootId;
            uint64_t cid = item.cid;
            uint64_t basechunkid = logicalRootMap[item.initialRootId];
            if (cid == rootId)
                    basechunkid = -1;
            Chunk_t &tmpChunk = item.tmpChunk;
            uint64_t initialRootId = item.initialRootId;

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

                    const double treeInsertThreshold =
                        ComputeTreeInsertThreshold(offline_dataWrite_->chunklist, tmpChunk.basechunkID);
                    if (static_cast<double>(tmpChunk.saveSize) >= treeInsertThreshold)
                    {
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

uint8_t *Design3_G::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void Design3_G::StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID)
{
    if (offline_dataWrite_->chunklist[FatherID].BeforeFit == HitID)
    {
        offline_dataWrite_->chunklist[FatherID].HitCount++;
    }
    else
    {
        offline_dataWrite_->chunklist[FatherID].BeforeFit = HitID;
        offline_dataWrite_->chunklist[FatherID].HitCount = 1;
    }
    if (offline_dataWrite_->chunklist[FatherID].HitCount > 4)
    {
        if (BasechunkID == FatherID)
            logicalRootMap[BasechunkID] = HitID;
    }
}

Chunk_t Design3_G::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk)
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
        // memcpy(basechunk.chunkPtr, cachedData.data(), basechunk.chunkSize);
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
            // std::vector<uint8_t> dataToCache(basechunk.chunkPtr, basechunk.chunkPtr + basechunk.chunkSize);
            chunkCache_.insert(BasechunkId, basechunk.chunkPtr, basechunk.chunkSize);
        }
    }

    if (basechunk.FirstChildID < 0) // if only one layer
        return basechunk;

    memcpy(CombinedBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    // greed init
    memcpy(MinBaseBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    resultchunk.chunkSize = basechunk.chunkSize;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = basechunk.chunkID;
    resultchunk.FirstChildID = basechunk.FirstChildID;

    xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk.chunkPtr, basechunk.chunkSize, &resultchunk.saveSize, deltaMaxChunkBuffer); //*** resultchunk.saveSize save tmpMinDeltaSize only here

    if (basechunk.loadFromDisk)
        free(basechunk.chunkPtr); // free base chunk memory

    bool end = false;
    uint64_t tmpsaveSize = 0;
    while (!end && resultchunk.FirstChildID >= 0)
    {
        uint64_t tmpFatherID = resultchunk.chunkID;
        uint64_t tmpChildID = resultchunk.chunkID;

        uint64_t childId = resultchunk.FirstChildID;
        Chunk_t TmpChildChunk = offline_dataWrite_->Get_Chunk_MetaInfo(childId);
        uint8_t *basechunk_ptr = nullptr;
        cacheAccessCount++;
        bool NeedFreeChild = false;
        if (uint8_t *cachedPtr2 = chunkCache_.tryGet(childId, cachedSize); cachedPtr2 != nullptr)
        {
            cacheHitCount++;
            // memcpy(chi_basechunk_ptr_cache, cachedData.data(), TmpChildChunk.chunkSize);
            basechunk_ptr = cachedPtr2;
            basechunk_size = TmpChildChunk.chunkSize;
            // Since we are not using TmpChildChunk.chunkPtr, no need to manage it
            TmpChildChunk.loadFromDisk = false;
        }
        else
        {
            TmpChildChunk = offline_dataWrite_->Get_Chunk_Info(childId);
            basechunk_ptr = xd3_decode(TmpChildChunk.chunkPtr, TmpChildChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
            NeedFreeChild = true;
            if (basechunk_ptr != nullptr)
            {
                // Cache the fully decoded chunk data
                // std::vector<uint8_t> dataToCache(basechunk_ptr, basechunk_ptr + basechunk_size);
                chunkCache_.insert(childId, basechunk_ptr, basechunk_size);
            }
        }

        // uint8_t *basechunk_ptr = xd3_decode(TmpChildChunk.chunkPtr, TmpChildChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
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
            free(TmpChildChunk.chunkPtr); // free child chunk memory
        if (NeedFreeChild)
            free(basechunk_ptr); // free base chunk memory

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
                // memcpy(bro_basechunk_ptr_cache, cachedData.data(), TmpBroChunk.chunkSize);
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
                    // std::vector<uint8_t> dataToCache(bro_basechunk_ptr, bro_basechunk_ptr + basechunk_size);
                    chunkCache_.insert(broId, bro_basechunk_ptr, basechunk_size);
                }
            }

            xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, bro_basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer); //*** resultchunk.saveSize save tmpMinDeltaSize only here
            if (tmpsaveSize < resultchunk.saveSize)
            {
                resultchunk.saveSize = tmpsaveSize;
                resultchunk.chunkID = TmpBroChunk.chunkID;
                resultchunk.chunkSize = TmpBroChunk.chunkSize;
                resultchunk.FirstChildID = TmpBroChunk.FirstChildID;
                memcpy(MinBaseBuffer, bro_basechunk_ptr, TmpBroChunk.chunkSize);
            }
            if (TmpBroChunk.loadFromDisk)
                free(TmpBroChunk.chunkPtr); // free bro chunk memory
            if (NeedFreeBro)
                free(bro_basechunk_ptr); // free base chunk memory
        }
        StatsHit(tmpFatherID, resultchunk.chunkID, BasechunkId);
        if (resultchunk.chunkID == tmpChildID)
        {
            end = true; // no more child or bro
        }
        else
        {
            basechunk.chunkSize = resultchunk.chunkSize;
            memcpy(CombinedBuffer, resultchunk.chunkPtr, resultchunk.chunkSize); // CombineBuffer is FatherNode
        }
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    return resultchunk;
}
