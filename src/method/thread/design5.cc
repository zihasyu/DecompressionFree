#include "../../../include/Thread/design5.h"
#include <cstdint>

namespace
{
void ResetChunkForAppend(Chunk_t &chunk)
{
    chunk.basechunkID = -1;
    chunk.FirstChildID = -1;
    chunk.FirstBroID = -1;
    chunk.BeforeFit = -1;
    chunk.HitCount = 0;
    chunk.deltaFlag = NO_DELTA;
}

int FindNextLiveNode(const AbsMethod *method, const std::vector<Chunk_t> &chunklist, int startId)
{
    int currentId = startId;
    while (currentId >= 0)
    {
        if (method->ShouldKeepChunk(static_cast<uint64_t>(currentId)))
        {
            return currentId;
        }
        currentId = chunklist[currentId].FirstBroID;
    }
    return -1;
}
} // namespace

Design5::Design5()
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

Design5::~Design5()
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

void Design5::SetAppendRange(uint64_t appendStart, uint64_t appendEnd)
{
    appendStart_ = appendStart;
    appendEnd_ = appendEnd;
}

Chunk_t Design5::LoadSourceChunk(uint64_t chunkId)
{
    Chunk_t metaChunk = dataWrite_->Get_Chunk_MetaInfo(chunkId);
    Chunk_t restoredChunk;

    if (metaChunk.basechunkID >= 0)
    {
        restoredChunk = dataWrite_->xd3_recursive_restore_offline_time(chunkId);
    }
    else
    {
        restoredChunk = dataWrite_->Get_Chunk_Info(chunkId);
    }

    Chunk_t resultChunk = metaChunk;
    resultChunk.chunkPtr = (uint8_t *)malloc(metaChunk.chunkSize);
    if (resultChunk.chunkPtr != nullptr && restoredChunk.chunkPtr != nullptr)
    {
        memcpy(resultChunk.chunkPtr, restoredChunk.chunkPtr, metaChunk.chunkSize);
    }
    resultChunk.loadFromDisk = true;
    ResetChunkForAppend(resultChunk);

    if (restoredChunk.loadFromDisk && restoredChunk.chunkPtr != nullptr)
    {
        free(restoredChunk.chunkPtr);
    }
    return resultChunk;
}

void Design5::ResetSearchState()
{
    logicalRootMap.clear();
    lastChildMap.clear();
    chunkCache_.clear();
    cacheHitCount = 0;
    cacheAccessCount = 0;

    if (offline_dataWrite_ == nullptr)
    {
        return;
    }

    for (auto &chunk : offline_dataWrite_->chunklist)
    {
        chunk.BeforeFit = -1;
        chunk.HitCount = 0;
    }
}

void Design5::AppendChild(uint64_t parentId, uint64_t childId)
{
    int firstLiveChildId = FindNextLiveNode(this, offline_dataWrite_->chunklist, offline_dataWrite_->chunklist[parentId].FirstChildID);
    if (firstLiveChildId < 0)
    {
        offline_dataWrite_->chunklist[parentId].FirstChildID = childId;
        lastChildMap[parentId] = childId;
        return;
    }

    auto lastIt = lastChildMap.find(parentId);
    uint64_t lastChildId = 0;
    if (lastIt != lastChildMap.end())
    {
        lastChildId = lastIt->second;
    }
    else
    {
        lastChildId = firstLiveChildId;
        int nextLiveSiblingId = FindNextLiveNode(this, offline_dataWrite_->chunklist, offline_dataWrite_->chunklist[lastChildId].FirstBroID);
        while (nextLiveSiblingId >= 0)
        {
            lastChildId = static_cast<uint64_t>(nextLiveSiblingId);
            nextLiveSiblingId = FindNextLiveNode(this, offline_dataWrite_->chunklist, offline_dataWrite_->chunklist[lastChildId].FirstBroID);
        }
    }

    offline_dataWrite_->chunklist[lastChildId].FirstBroID = childId;
    lastChildMap[parentId] = childId;
}

template <typename T>
class ThreadSafeQueue5
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

struct RestoredChunk5
{
    uint64_t rootId;
    uint64_t cid;
    Chunk_t tmpChunk;
    uint64_t initialRootId;
};

void Design5::ProcessTrace()
{
    using namespace std;
    using namespace std::chrono;

    if (rootChunkMap == nullptr)
    {
        cout << "rootChunkMap is nullptr in Design5::ProcessTrace" << endl;
        return;
    }
    if (offline_dataWrite_ == nullptr || dataWrite_ == nullptr)
    {
        cout << "dataWrite is nullptr in Design5::ProcessTrace" << endl;
        return;
    }
    if (appendStart_ >= appendEnd_)
    {
        cout << "append range is empty in Design5::ProcessTrace" << endl;
        return;
    }

    ResetSearchState();
    ThreadSafeQueue5<RestoredChunk5> chunkQueue;

    std::map<uint64_t, std::vector<uint64_t>> sortedRootChunkMap;
    for (const auto &pair : *rootChunkMap)
    {
        std::vector<uint64_t> batchChunkIds;
        batchChunkIds.reserve(pair.second.size());
        for (uint64_t chunkId : pair.second)
        {
            if (chunkId >= appendStart_ && chunkId < appendEnd_ && ShouldKeepChunk(chunkId))
            {
                batchChunkIds.push_back(chunkId);
            }
        }
        if (!batchChunkIds.empty())
        {
            sortedRootChunkMap.emplace(pair.first, std::move(batchChunkIds));
            logicalRootMap[pair.first] = ShouldKeepChunk(pair.first) ? pair.first : static_cast<uint64_t>(-1);
        }
    }

    if (sortedRootChunkMap.empty())
    {
        cout << "no new root entries for append range [" << appendStart_ << ", " << appendEnd_ << ")" << endl;
        return;
    }

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
                Chunk_t tmpChunk = LoadSourceChunk(cid);
                auto endRestoreChunk = high_resolution_clock::now();
                RestoreChunkTime += endRestoreChunk - startRestoreChunk;

                auto subRootIt = sortedRootChunkMap.find(cid);
                if (subRootIt != sortedRootChunkMap.end() && !subRootIt->second.empty() && cid != subRootIt->second[0])
                {
                    subRoots.push_back(cid);
                }

                chunkQueue.push(RestoredChunk5{rootId, cid, tmpChunk, item.initialRootId});
            }

            for (uint64_t subRoot : subRoots) {
                process_tree(subRoot);
            }
        };

        for (const auto &pair : sortedRootChunkMap)
        {
            if (!pair.second.empty())
            {
                process_tree(pair.first);
            }
        }
        chunkQueue.set_finished(); });

    std::thread processThread([&]()
                              {
        RestoredChunk5 item;
        while (chunkQueue.pop(item))
        {
            uint64_t rootId = item.rootId;
            uint64_t cid = item.cid;
            uint64_t basechunkid = logicalRootMap[item.initialRootId];
            if (cid == rootId)
                basechunkid = -1;
            const bool needsRootBootstrap = (logicalRootMap[item.initialRootId] == static_cast<uint64_t>(-1));
            Chunk_t &tmpChunk = item.tmpChunk;
            ResetChunkForAppend(tmpChunk);

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

                    if (tmpChunk.saveSize >= TREE_INSERT_SAVE_THRESHOLD)
                    {
                        AppendChild(tmpChunk.basechunkID, tmpChunk.chunkID);
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
            if (needsRootBootstrap)
            {
                logicalRootMap[item.initialRootId] = tmpChunk.chunkID;
            }
        } });

    restoreThread.join();
    processThread.join();

    const double cacheHitRate = cacheAccessCount == 0 ? 0.0 : (double)cacheHitCount / (double)cacheAccessCount;
    cout << "lru cache hit rate: " << cacheHitRate << " cacheHitCount " << cacheHitCount << " cacheAccessCount " << cacheAccessCount << endl;
    return;
}

uint8_t *Design5::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void Design5::StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID)
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

Chunk_t Design5::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk)
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
            basechunk = offline_dataWrite_->xd3_recursive_restore_offline_time(BasechunkId);
        }
        if (basechunk.chunkPtr != nullptr)
        {
            chunkCache_.insert(BasechunkId, basechunk.chunkPtr, basechunk.chunkSize);
        }
    }

    int firstLiveChildId = FindNextLiveNode(this, offline_dataWrite_->chunklist, basechunk.FirstChildID);
    if (firstLiveChildId < 0)
        return basechunk;

    memcpy(CombinedBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    memcpy(MinBaseBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    resultchunk.chunkSize = basechunk.chunkSize;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = basechunk.chunkID;
    resultchunk.FirstChildID = firstLiveChildId;

    xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk.chunkPtr, basechunk.chunkSize, &resultchunk.saveSize, deltaMaxChunkBuffer);

    if (basechunk.loadFromDisk)
        free(basechunk.chunkPtr);

    bool end = false;
    uint64_t tmpsaveSize = 0;
    while (!end && resultchunk.FirstChildID >= 0)
    {
        uint64_t tmpFatherID = resultchunk.chunkID;
        uint64_t tmpChildID = resultchunk.chunkID;

        int childId = FindNextLiveNode(this, offline_dataWrite_->chunklist, resultchunk.FirstChildID);
        if (childId < 0)
        {
            break;
        }
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
        int broId = FindNextLiveNode(this, offline_dataWrite_->chunklist, TmpBroChunk.FirstBroID);
        while (broId >= 0)
        {
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
            broId = FindNextLiveNode(this, offline_dataWrite_->chunklist, TmpBroChunk.FirstBroID);
        }
        StatsHit(tmpFatherID, resultchunk.chunkID, BasechunkId);
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
