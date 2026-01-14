#include "../../../include/Offline/offline_tree_Feature_lru.h"

const size_t TREE_INSERT_SAVE_THRESHOLD = 128;

OfflineTreeFeatureLru::OfflineTreeFeatureLru()
    : chunkCache(1024) // 在构造函数初始化列表中初始化缓存容量
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

OfflineTreeFeatureLru::~OfflineTreeFeatureLru()
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

void OfflineTreeFeatureLru::ProcessTrace()
{
    // string tmpChunkContent;
    // SuperFeatures superfeature;
    size_t nextVersionEndPointIndex = 0;

    std::map<uint64_t, const std::vector<uint64_t> &> sortedRootChunkMap;
    for (const auto &pair : *rootChunkMap)
    {
        sortedRootChunkMap.insert(pair);
    }

    // [CHANGE] 遍历新创建的、有序的 sortedRootChunkMap
    for (const auto &pair : sortedRootChunkMap)
    {
        uint64_t rootId = pair.first;
        const std::vector<uint64_t> &chunkIds = pair.second;
        if (chunkIds.empty() || rootId != chunkIds[0])
        {
            continue;
        }
        // [FIX] 对主根进行 logicalRootMap 的初始化
        logicalRootMap[rootId] = rootId;
        // --- 栈式遍历，现在包含迭代器和逻辑根节点 ---
        std::stack<std::vector<uint64_t>::const_iterator> iterStack;
        std::stack<std::vector<uint64_t>::const_iterator> endStack;
        std::stack<uint64_t> initialRootStack; // [NEW] 用于跟踪每一层的初始根ID

        iterStack.push(chunkIds.begin());
        endStack.push(chunkIds.end());
        initialRootStack.push(rootId); // 初始根节点
        while (!iterStack.empty())
        {
            auto &currentIter = iterStack.top();
            auto &currentEnd = endStack.top();
            uint64_t currentInitialRootId = initialRootStack.top(); // 获取当前层的初始根ID
            if (currentIter == currentEnd)
            {
                iterStack.pop();
                endStack.pop();
                initialRootStack.pop(); // 同步出栈
                continue;
            }

            uint64_t cid = *currentIter;
            currentIter++;

            // 1. Restore the chunk content to its original form
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
                tmpChunk = rawChunk;
                tmpChunk.chunkPtr = (uint8_t *)malloc(tmpChunk.chunkSize);
                if (tmpChunk.chunkPtr != nullptr && rawChunk.chunkPtr != nullptr)
                {
                    memcpy(tmpChunk.chunkPtr, rawChunk.chunkPtr, tmpChunk.chunkSize);
                }
                tmpChunk.loadFromDisk = true;
                if (rawChunk.loadFromDisk)
                {
                    free(rawChunk.chunkPtr);
                }
            }

            // 1.5. 检查并处理“插队”
            auto subRootIt = sortedRootChunkMap.find(cid);
            if (subRootIt != sortedRootChunkMap.end() && subRootIt->first != subRootIt->second[0])
            {
                const std::vector<uint64_t> &subChunkIds = subRootIt->second;
                iterStack.push(subChunkIds.begin());
                endStack.push(subChunkIds.end());
                initialRootStack.push(cid); // 将子根ID作为新一层的初始根压栈
                logicalRootMap[cid] = cid;  // 初始化子根的逻辑根为它自己
                // std::cout << "    !! [DEBUG] Chunk " << cid << " is a Sub-root. Pushing its group to stack. Context switched to " << cid << "." << std::endl;
            }

            uint64_t basechunkid = logicalRootMap[currentInitialRootId];
            if (cid == rootId)
                basechunkid = -1; // 根节点没有基准块

            // 3. Re-process the chunk
            if (basechunkid != -1)
            // A potential base chunk was found
            {
                // Use CutGreedy to find the best base within the delta tree
                auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk);
                uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

                if (RestoreBasechunk.loadFromDisk)
                    free(RestoreBasechunk.chunkPtr);

                if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
                {
                    // Delta is not effective, fallback to LZ4
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

                    // if (tmpChunk.chunkSize > 60)
                    //     table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    free(deltachunk);

                    // Insert into the destination offline_dataWrite_
                    if (tmpChunk.deltaFlag == NO_LZ4)
                        offline_dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                }
                else
                {
                    // Delta is successful
                    tmpChunk.deltaFlag = DELTA;
                    tmpChunk.basechunkID = RestoreBasechunk.chunkID;

                    // if (tmpChunk.chunkSize > 60)
                    //     table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);

                    if (tmpChunk.saveSize >= TREE_INSERT_SAVE_THRESHOLD)
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

                    // Insert into the destination offline_dataWrite_
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                }
            }
            else
            // No suitable base chunk found, treat as a new base chunk
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

                // if (tmpChunk.chunkSize > 60)
                //     table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                basechunkNum++;
                basechunkSize += tmpChunk.saveSize;

                // Insert into the destination offline_dataWrite_
                if (tmpChunk.deltaFlag == NO_LZ4)
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                else
                    offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
            }

            // Update statistics
            uniquechunkNum++; // In offline mode, every chunk is processed as "unique"
            uniquechunkSize += tmpChunk.saveSize;
            logicalchunkNum++;
            logicalchunkSize += tmpChunk.chunkSize;
        }
    }

    // Finalize
    ads_Version++;
    SFnum = basechunkNum * 3;
    return;
}

uint8_t *OfflineTreeFeatureLru::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void OfflineTreeFeatureLru::StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID)
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

Chunk_t OfflineTreeFeatureLru::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;

    Chunk_t basechunk = offline_dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    if (basechunk.basechunkID < 0)
    {
        SetTime(startIO);
        basechunk = offline_dataWrite_->Get_Chunk_Info(BasechunkId);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
        // basechunk = xd3_recursive_restore_BL_time(BasechunkId);
    }
    else
    {
        basechunk = xd3_recursive_restore_offline_time(BasechunkId);
        // cout << "basechunk.ChunkID is " << basechunk.chunkID << endl;
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
    }

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

        SetTime(startIO);
        Chunk_t TmpChildChunk = offline_dataWrite_->Get_Chunk_Info(resultchunk.FirstChildID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        uint8_t *basechunk_ptr = xd3_decode(TmpChildChunk.chunkPtr, TmpChildChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
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
        free(basechunk_ptr);              // free base chunk memory

        Chunk_t TmpBroChunk = TmpChildChunk;
        while (TmpBroChunk.FirstBroID >= 0)
        {
            SetTime(startIO);
            TmpBroChunk = offline_dataWrite_->Get_Chunk_Info(TmpBroChunk.FirstBroID);
            SetTime(endIO);
            SetTime(startIO, endIO, IOTime);
            uint8_t *basechunk_ptr = xd3_decode(TmpBroChunk.chunkPtr, TmpBroChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
            xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer); //*** resultchunk.saveSize save tmpMinDeltaSize only here
            if (tmpsaveSize < resultchunk.saveSize)
            {
                resultchunk.saveSize = tmpsaveSize;
                resultchunk.chunkID = TmpBroChunk.chunkID;
                resultchunk.chunkSize = TmpBroChunk.chunkSize;
                resultchunk.FirstChildID = TmpBroChunk.FirstChildID;
                memcpy(MinBaseBuffer, basechunk_ptr, TmpBroChunk.chunkSize);
            }
            if (TmpBroChunk.loadFromDisk)
                free(TmpBroChunk.chunkPtr); // free bro chunk memory
            free(basechunk_ptr);
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