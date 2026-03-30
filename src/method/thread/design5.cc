#include "../../../include/Thread/design5.h"
#include <cstdint>
#include <filesystem>
#include <unordered_set>

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

void ResetDirectory(const std::string &path)
{
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::remove_all(path, ec);
    ec.clear();
    fs::create_directories(path, ec);
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
    delete[] SFindex;
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

std::string Design5::PrepareNextGenerationPath()
{
    const std::string path = "./OfflineContainers/gen" + std::to_string(generationId_ % 2) + "/";
    generationId_++;
    ResetDirectory(path);
    return path;
}

bool Design5::ChunkExists(const dataWrite *writer, uint64_t chunkId) const
{
    return writer != nullptr &&
           chunkId < writer->chunklist.size() &&
           writer->chunklist[chunkId].chunkSize > 0;
}

bool Design5::IsSearchableChunk(const Chunk_t &chunk) const
{
    if (chunk.chunkSize == 0)
    {
        return false;
    }
    if (chunk.basechunkID < 0)
    {
        return true;
    }
    return chunk.deltaFlag == DELTA && chunk.saveSize >= TREE_INSERT_SAVE_THRESHOLD;
}

bool Design5::HasAcyclicBaseChain(dataWrite *writer, uint64_t chunkId) const
{
    if (!ChunkExists(writer, chunkId))
    {
        return false;
    }

    std::unordered_set<uint64_t> visited;
    int currentId = static_cast<int>(chunkId);
    while (currentId >= 0)
    {
        const uint64_t current = static_cast<uint64_t>(currentId);
        if (!ChunkExists(writer, current))
        {
            return false;
        }
        if (!visited.insert(current).second)
        {
            return false;
        }

        currentId = writer->Get_Chunk_MetaInfo(currentId).basechunkID;
    }
    return true;
}

bool Design5::OwnsSuperFeature(uint64_t chunkId, super_feature_t sf) const
{
    auto it = searchableChunkSFs_.find(chunkId);
    if (it == searchableChunkSFs_.end())
    {
        return false;
    }
    for (const auto &feature : it->second)
    {
        if (feature == sf)
        {
            return true;
        }
    }
    return false;
}

void Design5::RecordSearchableChunkSF(uint64_t chunkId, const Chunk_t &rawChunk)
{
    if (rawChunk.chunkPtr == nullptr || rawChunk.chunkSize <= 60)
    {
        searchableChunkSFs_.erase(chunkId);
        return;
    }
    std::string chunkContent(reinterpret_cast<const char *>(rawChunk.chunkPtr), rawChunk.chunkSize);
    searchableChunkSFs_[chunkId] = table.feature_generator_.GenerateSuperFeatures(chunkContent);
}

void Design5::RegisterNewSearchableChunk(uint64_t chunkId, const Chunk_t &rawChunk)
{
    if (rawChunk.chunkSize <= 60)
    {
        return;
    }

    RecordSearchableChunkSF(chunkId, rawChunk);
    const auto sfIt = searchableChunkSFs_.find(chunkId);
    if (sfIt == searchableChunkSFs_.end())
    {
        return;
    }

    for (const auto &sf : sfIt->second)
    {
        if (table.Tree_SFIndex.find(sf) == table.Tree_SFIndex.end())
        {
            table.Tree_SFIndex[sf] = chunkId;
        }
    }
}

int Design5::ResolveReplacementBase(dataWrite *writer, int baseChunkId) const
{
    int currentId = baseChunkId;
    while (currentId >= 0)
    {
        if (!ChunkExists(writer, static_cast<uint64_t>(currentId)))
        {
            return -1;
        }

        if (ShouldKeepChunk(static_cast<uint64_t>(currentId)))
        {
            const Chunk_t currentMeta = writer->Get_Chunk_MetaInfo(currentId);
            return IsSearchableChunk(currentMeta) ? currentId : -1;
        }

        currentId = writer->Get_Chunk_MetaInfo(currentId).basechunkID;
    }
    return -1;
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

Chunk_t Design5::RestoreChunkFromWriter(dataWrite *writer, uint64_t chunkId)
{
    Chunk_t emptyChunk{};
    emptyChunk.chunkID = chunkId;
    emptyChunk.chunkPtr = nullptr;
    emptyChunk.chunkSize = 0;
    emptyChunk.saveSize = 0;
    emptyChunk.basechunkID = -1;
    emptyChunk.loadFromDisk = false;

    if (!ChunkExists(writer, chunkId))
    {
        return emptyChunk;
    }

    Chunk_t metaChunk = writer->Get_Chunk_MetaInfo(chunkId);
    if (metaChunk.basechunkID >= 0)
    {
        return writer->xd3_recursive_restore_offline_time(chunkId);
    }
    return writer->Get_Chunk_Info(chunkId);
}

void Design5::ResetSearchState()
{
    logicalRootMap.clear();
    lastChildMap.clear();
    chunkCache_.clear();
    searchableChunkSFs_.clear();
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
    if (offline_dataWrite_ == nullptr || parentId >= offline_dataWrite_->chunklist.size())
    {
        return;
    }

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

void Design5::ResetOfflineStatsForRebuild()
{
    totalLogicalSize = 0;
    totalCompressedSize = 0;
    logicalchunkNum = 0;
    uniquechunkNum = 0;
    basechunkNum = 0;
    deltachunkNum = 0;
    logicalchunkSize = 0;
    uniquechunkSize = 0;
    basechunkSize = 0;
    deltachunkSize = 0;
}

bool Design5::RewriteChunkAsLz4Base(const Chunk_t &sourceMeta, Chunk_t &rawChunk)
{
    Chunk_t rewritten = sourceMeta;
    ResetChunkForAppend(rewritten);
    rewritten.chunkID = sourceMeta.chunkID;
    rewritten.chunkSize = sourceMeta.chunkSize;
    rewritten.basechunkID = -1;
    rewritten.loadFromDisk = rawChunk.loadFromDisk;
    rewritten.chunkPtr = rawChunk.chunkPtr;

    const int lz4Size = LZ4_compress_fast(reinterpret_cast<const char *>(rawChunk.chunkPtr),
                                          reinterpret_cast<char *>(lz4ChunkBuffer),
                                          rawChunk.chunkSize, rawChunk.chunkSize, 3);
    if (lz4Size > 0)
    {
        rewritten.deltaFlag = NO_DELTA;
        rewritten.saveSize = lz4Size;
        offline_dataWrite_->Chunk_Insert(rewritten, lz4ChunkBuffer);
    }
    else
    {
        rewritten.deltaFlag = NO_LZ4;
        rewritten.saveSize = rawChunk.chunkSize;
        offline_dataWrite_->Chunk_Insert(rewritten);
    }

    basechunkNum++;
    basechunkSize += rewritten.saveSize;
    uniquechunkNum++;
    uniquechunkSize += rewritten.saveSize;
    logicalchunkNum++;
    logicalchunkSize += rewritten.chunkSize;
    rawChunk.chunkPtr = nullptr;
    rawChunk.loadFromDisk = false;
    return true;
}

bool Design5::RewriteChunkWithOriginalDelta(dataWrite *sourceWriter, const Chunk_t &sourceMeta, const Chunk_t &rawChunk)
{
    if (sourceMeta.basechunkID < 0 ||
        !ChunkExists(offline_dataWrite_, static_cast<uint64_t>(sourceMeta.basechunkID)))
    {
        return false;
    }

    Chunk_t storedChunk = sourceWriter->Get_Chunk_Info(sourceMeta.chunkID);
    Chunk_t rewritten = sourceMeta;
    ResetChunkForAppend(rewritten);
    rewritten.chunkID = sourceMeta.chunkID;
    rewritten.chunkSize = sourceMeta.chunkSize;
    rewritten.saveSize = sourceMeta.saveSize;
    rewritten.basechunkID = sourceMeta.basechunkID;
    rewritten.deltaFlag = DELTA;
    rewritten.chunkPtr = static_cast<uint8_t *>(malloc(sourceMeta.saveSize));
    rewritten.loadFromDisk = true;
    if (rewritten.chunkPtr == nullptr)
    {
        if (storedChunk.loadFromDisk && storedChunk.chunkPtr != nullptr)
        {
            free(storedChunk.chunkPtr);
        }
        return false;
    }
    memcpy(rewritten.chunkPtr, storedChunk.chunkPtr, sourceMeta.saveSize);

    if (IsSearchableChunk(rewritten))
    {
        AppendChild(rewritten.basechunkID, rewritten.chunkID);
    }
    offline_dataWrite_->Chunk_Insert(rewritten);

    deltachunkNum++;
    deltachunkSize += rewritten.saveSize;
    uniquechunkNum++;
    uniquechunkSize += rewritten.saveSize;
    logicalchunkNum++;
    logicalchunkSize += rewritten.chunkSize;

    if (storedChunk.loadFromDisk && storedChunk.chunkPtr != nullptr)
    {
        free(storedChunk.chunkPtr);
    }
    return true;
}

bool Design5::RewriteChunkWithReplacementBase(const Chunk_t &sourceMeta, Chunk_t &rawChunk, int replacementBaseId)
{
    if (replacementBaseId < 0 || !ChunkExists(offline_dataWrite_, static_cast<uint64_t>(replacementBaseId)))
    {
        return RewriteChunkAsLz4Base(sourceMeta, rawChunk);
    }

    Chunk_t replacementBase = offline_dataWrite_->Get_Chunk_MetaInfo(replacementBaseId);
    if (replacementBase.basechunkID >= 0)
    {
        replacementBase = offline_dataWrite_->xd3_recursive_restore_offline_time(replacementBaseId);
    }
    else
    {
        replacementBase = offline_dataWrite_->Get_Chunk_Info(replacementBaseId);
    }

    if (replacementBase.chunkPtr == nullptr || replacementBase.chunkSize == 0)
    {
        if (replacementBase.loadFromDisk && replacementBase.chunkPtr != nullptr)
        {
            free(replacementBase.chunkPtr);
        }
        return RewriteChunkAsLz4Base(sourceMeta, rawChunk);
    }

    size_t rewrittenSize = 0;
    uint8_t *deltaChunk = xd3_encode(rawChunk.chunkPtr, rawChunk.chunkSize,
                                     replacementBase.chunkPtr, replacementBase.chunkSize,
                                     &rewrittenSize, deltaMaxChunkBuffer);

    if (replacementBase.loadFromDisk && replacementBase.chunkPtr != nullptr)
    {
        free(replacementBase.chunkPtr);
    }

    if (rewrittenSize <= 0 || rewrittenSize >= rawChunk.chunkSize || rewrittenSize < TREE_INSERT_SAVE_THRESHOLD)
    {
        if (deltaChunk != nullptr)
        {
            free(deltaChunk);
        }
        return RewriteChunkAsLz4Base(sourceMeta, rawChunk);
    }

    Chunk_t rewritten = sourceMeta;
    ResetChunkForAppend(rewritten);
    rewritten.chunkID = sourceMeta.chunkID;
    rewritten.chunkSize = sourceMeta.chunkSize;
    rewritten.saveSize = rewrittenSize;
    rewritten.basechunkID = replacementBaseId;
    rewritten.deltaFlag = DELTA;
    rewritten.chunkPtr = rawChunk.chunkPtr;
    rewritten.loadFromDisk = rawChunk.loadFromDisk;

    memcpy(rewritten.chunkPtr, deltaChunk, rewrittenSize);
    free(deltaChunk);

    AppendChild(rewritten.basechunkID, rewritten.chunkID);
    offline_dataWrite_->Chunk_Insert(rewritten);

    deltachunkNum++;
    deltachunkSize += rewritten.saveSize;
    uniquechunkNum++;
    uniquechunkSize += rewritten.saveSize;
    logicalchunkNum++;
    logicalchunkSize += rewritten.chunkSize;
    rawChunk.chunkPtr = nullptr;
    rawChunk.loadFromDisk = false;
    return true;
}

int Design5::FindReplacementEntryInSubtree(dataWrite *writer, uint64_t rootId, super_feature_t sf) const
{
    if (!ChunkExists(writer, rootId))
    {
        return -1;
    }

    std::queue<uint64_t> q;
    Chunk_t rootMeta = writer->Get_Chunk_MetaInfo(rootId);
    if (rootMeta.FirstChildID >= 0)
    {
        q.push(static_cast<uint64_t>(rootMeta.FirstChildID));
    }

    while (!q.empty())
    {
        uint64_t currentId = q.front();
        q.pop();

        while (true)
        {
            if (ChunkExists(writer, currentId))
            {
                if (ShouldKeepChunk(currentId) && OwnsSuperFeature(currentId, sf))
                {
                    return static_cast<int>(currentId);
                }

                Chunk_t currentMeta = writer->Get_Chunk_MetaInfo(currentId);
                if (currentMeta.FirstChildID >= 0)
                {
                    q.push(static_cast<uint64_t>(currentMeta.FirstChildID));
                }
                if (currentMeta.FirstBroID >= 0)
                {
                    currentId = static_cast<uint64_t>(currentMeta.FirstBroID);
                    continue;
                }
            }
            break;
        }
    }

    return -1;
}

void Design5::RepairTreeIndexFromHistoricalState(dataWrite *sourceWriter,
                                                 const std::unordered_map<super_feature_t, uint64_t> &oldTreeIndex)
{
    table.Tree_SFIndex.clear();

    for (const auto &[sf, oldEntry] : oldTreeIndex)
    {
        if (!ChunkExists(sourceWriter, oldEntry))
        {
            continue;
        }

        if (ShouldKeepChunk(oldEntry) &&
            ChunkExists(offline_dataWrite_, oldEntry) &&
            IsSearchableChunk(offline_dataWrite_->Get_Chunk_MetaInfo(oldEntry)) &&
            OwnsSuperFeature(oldEntry, sf))
        {
            table.Tree_SFIndex[sf] = oldEntry;
            continue;
        }

        const int replacementId = FindReplacementEntryInSubtree(sourceWriter, oldEntry, sf);
        if (replacementId >= 0 &&
            ChunkExists(offline_dataWrite_, static_cast<uint64_t>(replacementId)) &&
            IsSearchableChunk(offline_dataWrite_->Get_Chunk_MetaInfo(replacementId)))
        {
            table.Tree_SFIndex[sf] = static_cast<uint64_t>(replacementId);
        }
    }
}

void Design5::RewriteKeptHistoricalChunks(dataWrite *sourceWriter,
                                          const std::unordered_map<super_feature_t, uint64_t> &oldTreeIndex)
{
    if (sourceWriter == nullptr)
    {
        return;
    }

    std::vector<uint8_t> rewriteState(sourceWriter->chunklist.size(), 0);
    std::function<void(uint64_t)> rewriteChunk = [&](uint64_t chunkId)
    {
        if (!ShouldKeepChunk(chunkId))
        {
            return;
        }
        if (chunkId >= rewriteState.size())
        {
            rewriteState.resize(chunkId + 1, 0);
        }
        if (rewriteState[chunkId] == 2)
        {
            return;
        }
        if (rewriteState[chunkId] == 1)
        {
            return;
        }

        rewriteState[chunkId] = 1;
        const bool hasHistoricalChunk = ChunkExists(sourceWriter, chunkId);
        const bool hasInlineChunk = ChunkExists(dataWrite_, chunkId);
        if (!hasHistoricalChunk && !hasInlineChunk)
        {
            cout << "design5 rewrite error, chunk " << chunkId
                 << " is kept but missing from both offline and inline sources" << endl;
            rewriteState[chunkId] = 2;
            return;
        }

        dataWrite *metaWriter = hasHistoricalChunk ? sourceWriter : dataWrite_;
        const Chunk_t sourceMeta = metaWriter->Get_Chunk_MetaInfo(chunkId);
        const bool hasValidSourceChain = sourceMeta.basechunkID < 0 || HasAcyclicBaseChain(metaWriter, chunkId);
        if (sourceMeta.basechunkID >= 0 &&
            ShouldKeepChunk(static_cast<uint64_t>(sourceMeta.basechunkID)))
        {
            rewriteChunk(static_cast<uint64_t>(sourceMeta.basechunkID));
        }

        Chunk_t rawChunk = LoadSourceChunk(chunkId);
        if (rawChunk.chunkPtr == nullptr || rawChunk.chunkSize == 0)
        {
            if (hasValidSourceChain)
            {
                cout << "design5 rewrite error, failed to restore historical chunk " << chunkId << endl;
            }
            else
            {
                cout << "design5 rewrite warning, historical chunk " << chunkId
                     << " has invalid base chain and cannot be restored for rewrite" << endl;
            }
            rewriteState[chunkId] = 2;
            return;
        }
        bool hasRawSF = false;
        SuperFeatures rawSuperFeatures;
        if (rawChunk.chunkPtr != nullptr && rawChunk.chunkSize > 60)
        {
            const std::string rawChunkContent(reinterpret_cast<const char *>(rawChunk.chunkPtr), rawChunk.chunkSize);
            rawSuperFeatures = table.feature_generator_.GenerateSuperFeatures(rawChunkContent);
            hasRawSF = true;
        }
        const bool baseAlive = sourceMeta.basechunkID >= 0 &&
                               ChunkExists(sourceWriter, static_cast<uint64_t>(sourceMeta.basechunkID)) &&
                               ShouldKeepChunk(static_cast<uint64_t>(sourceMeta.basechunkID));

        if (sourceMeta.basechunkID >= 0 && sourceMeta.deltaFlag == DELTA)
        {
            const bool baseReady = hasValidSourceChain &&
                                   baseAlive &&
                                   ChunkExists(offline_dataWrite_, static_cast<uint64_t>(sourceMeta.basechunkID));
            if (baseReady && RewriteChunkWithReplacementBase(sourceMeta, rawChunk, sourceMeta.basechunkID))
            {
            }
            else
            {
                const int replacementBaseId = ResolveReplacementBase(metaWriter, sourceMeta.basechunkID);
                RewriteChunkWithReplacementBase(sourceMeta, rawChunk, replacementBaseId);
            }
        }
        else
        {
            RewriteChunkAsLz4Base(sourceMeta, rawChunk);
        }

        const Chunk_t rebuiltMeta = offline_dataWrite_->Get_Chunk_MetaInfo(chunkId);
        if (IsSearchableChunk(rebuiltMeta) && hasRawSF)
        {
            searchableChunkSFs_[chunkId] = rawSuperFeatures;
        }
        else
        {
            searchableChunkSFs_.erase(chunkId);
        }

        if (rawChunk.loadFromDisk && rawChunk.chunkPtr != nullptr)
        {
            free(rawChunk.chunkPtr);
        }

        rewriteState[chunkId] = 2;
    };

    for (uint64_t chunkId = 0; chunkId < sourceWriter->chunklist.size(); ++chunkId)
    {
        rewriteChunk(chunkId);
    }

    RepairTreeIndexFromHistoricalState(sourceWriter, oldTreeIndex);
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

    dataWrite *sourceWriter = offline_dataWrite_;
    const auto oldTreeIndex = table.Tree_SFIndex;
    auto *nextWriter = new dataWrite();
    nextWriter->setContainerPath(PrepareNextGenerationPath());
    offline_dataWrite_ = nextWriter;

    ResetOfflineStatsForRebuild();
    ResetSearchState();

    if (sourceWriter != nullptr && !sourceWriter->chunklist.empty())
    {
        RewriteKeptHistoricalChunks(sourceWriter, oldTreeIndex);
    }
    else
    {
        table.Tree_SFIndex.clear();
    }

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
            bool hasSearchableSF = false;
            SuperFeatures rawSuperFeatures;
            if (tmpChunk.chunkSize > 60)
            {
                const string rawChunkContent(reinterpret_cast<const char *>(tmpChunk.chunkPtr), tmpChunk.chunkSize);
                rawSuperFeatures = table.feature_generator_.GenerateSuperFeatures(rawChunkContent);
                hasSearchableSF = true;
            }
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
            if (IsSearchableChunk(tmpChunk) && hasSearchableSF)
            {
                searchableChunkSFs_[tmpChunk.chunkID] = rawSuperFeatures;
                for (const auto &sf : rawSuperFeatures)
                {
                    if (table.Tree_SFIndex.find(sf) == table.Tree_SFIndex.end())
                    {
                        table.Tree_SFIndex[sf] = tmpChunk.chunkID;
                    }
                }
            }
        } });

    restoreThread.join();
    processThread.join();

    if (sourceWriter != nullptr && sourceWriter != offline_dataWrite_)
    {
        delete sourceWriter;
    }

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
    Chunk_t resultchunk{};
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

    if (basechunk.chunkPtr == nullptr || basechunk.chunkSize == 0)
    {
        resultchunk.chunkID = BasechunkId;
        resultchunk.chunkPtr = nullptr;
        resultchunk.chunkSize = 0;
        resultchunk.saveSize = 0;
        resultchunk.basechunkID = -1;
        resultchunk.loadFromDisk = false;
        resultchunk.FirstChildID = -1;
        return resultchunk;
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
