#include "../../include/Tree/TreeCache2.h"

TreeCache2::TreeCache2()
    : chunk_cache_(CACHE_MAX_SIZE)
{
    // ... 您的构造函数现有代码 ...
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

TreeCache2::~TreeCache2()
{
    // ... 您的析构函数现有代码 ...
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
}

void TreeCache2::ProcessTrace()
{
    // ... 您的 ProcessTrace 现有代码 ...
    string tmpChunkHash;
    string tmpChunkContent;
    SuperFeatures superfeature;
    uint64_t HitSF;
    while (true)
    {
        string hashStr;
        hashStr.assign(CHUNK_HASH_SIZE, 0);
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            // --- 在此处添加命中率打印 ---
            double hit_rate = 0.0;
            if (cache2AccessCount > 0)
            {
                hit_rate = static_cast<double>(cache2HitCount) / cache2AccessCount * 100.0;
            }
            cout << "Cache Hit Rate: " << fixed << setprecision(2) << hit_rate << "% "
                 << "(" << cache2HitCount << " hits / " << cache2AccessCount << " accesses)" << endl;
            // --------------------------

            // outputMQ_->done_ = true;
            recieveQueue->done_ = false;
            ads_Version++;
            SFnum = basechunkNum * 3;
            break;
        }
        Chunk_t tmpChunk;
        if (recieveQueue->Pop(tmpChunk))
        {
            GenerateHash(mdCtx, tmpChunk.chunkPtr, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
            int tmpChunkid;
            int findRes = FP_Find(hashStr);
            if (findRes == -1)
            {
                // Unique chunk found
                tmpChunk.chunkID = uniquechunkNum;
                tmpChunk.deltaFlag = NO_DELTA;
                FP_Insert(hashStr, tmpChunk.chunkID);
                tmpChunkContent.assign((char *)tmpChunk.chunkPtr, tmpChunk.chunkSize);
                tmpChunkHash.assign((char *)hashBuf, CHUNK_HASH_SIZE);
                // TreeCutLayer get superfeature & get time
                uint64_t basechunkid = -1;
                // compute SF
                if (tmpChunk.chunkSize > 60)
                {
                    startSF = std::chrono::high_resolution_clock::now();
                    superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
                    endSF = std::chrono::high_resolution_clock::now();
                    SFTime += (endSF - startSF);

                    basechunkid = table.Tree_SF_Find(superfeature, HitSF);
                    // auto ret = table.GetSimilarRecordsKeys(tmpChunkHash);
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk, HitSF, superfeature);
                    uint8_t *deltachunk = xd3_encode_buffer(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);
                    if (RestoreBasechunk.loadFromDisk)
                        free(RestoreBasechunk.chunkPtr);

                    if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
                    {
                        cout << "delta no effective" << endl;
                        int tmpChunkLz4CompressSize = 0;
                        tmpChunkLz4CompressSize = LZ4_compress_fast((char *)tmpChunk.chunkPtr, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                        if (tmpChunkLz4CompressSize > 0)
                        {
                            tmpChunk.deltaFlag = NO_DELTA;
                            tmpChunk.saveSize = tmpChunkLz4CompressSize;
                        }
                        else
                        {
                            // cout << "lz4 compress error" << endl;
                            tmpChunk.deltaFlag = NO_LZ4;
                            tmpChunk.saveSize = tmpChunk.chunkSize;
                        }

                        tmpChunk.basechunkID = -1;
                        tmpChunkid = tmpChunk.chunkID;
                        if (tmpChunk.chunkSize > 60)
                            table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                        basechunkNum++;
                        basechunkSize += tmpChunk.saveSize;
                        LocalReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
                        free(deltachunk);
                        if (tmpChunk.deltaFlag == NO_LZ4)
                            // base chunk & Lz4 error
                            dataWrite_->Chunk_Insert(tmpChunk);
                        else
                            // base chunk &lz4 compress
                            dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                    }
                    else
                    {
                        tmpChunk.deltaFlag = DELTA;
                        // cout << "RestoreBasechunk.chunkID is " << RestoreBasechunk.chunkID << endl;
                        tmpChunk.basechunkID = RestoreBasechunk.chunkID;
                        // cout << "tmpChunk.savesize is " << tmpChunk.saveSize << endl;
                        if (tmpChunk.chunkSize > 60)
                            table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                        // cout << "tmpChunk.chunkID is " << tmpChunk.chunkID << endl;
                        // cout << "basechunkid is " << tmpChunk.basechunkID << endl;

                        if (dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID < 0)
                        {
                            // cout << "here 1" << endl;
                            dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID = tmpChunk.chunkID;
                        }
                        else
                        {
                            // cout << "here 2" << endl;
                            int broID = dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID;
                            while (dataWrite_->chunklist[broID].FirstBroID >= 0)
                                broID = dataWrite_->chunklist[broID].FirstBroID;
                            dataWrite_->chunklist[broID].FirstBroID = tmpChunk.chunkID;
                        }
                        memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                        StatsDelta(tmpChunk);
                        free(deltachunk);
                        // if (RestoreBasechunk.loadFromDisk)
                        //     free(RestoreBasechunk.chunkPtr);
                        dataWrite_->Chunk_Insert(tmpChunk);
                    }
                }
                // unique chunk & base chunk
                else
                {
                    int tmpChunkLz4CompressSize = 0;
                    tmpChunkLz4CompressSize = LZ4_compress_fast((char *)tmpChunk.chunkPtr, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                    if (tmpChunkLz4CompressSize > 0)
                    {
                        tmpChunk.deltaFlag = NO_DELTA;
                        tmpChunk.saveSize = tmpChunkLz4CompressSize;
                    }
                    else
                    {
                        // cout << "lz4 compress error" << endl;
                        tmpChunk.deltaFlag = NO_LZ4;
                        tmpChunk.saveSize = tmpChunk.chunkSize;
                    }

                    tmpChunk.basechunkID = -1;
                    tmpChunkid = tmpChunk.chunkID;
                    if (tmpChunk.chunkSize > 60)
                        table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    LocalReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
                    if (tmpChunk.deltaFlag == NO_LZ4)
                        // base chunk & Lz4 error
                        dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        // base chunk &lz4 compress
                        dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                }
                uniquechunkNum++;
                uniquechunkSize += tmpChunk.saveSize;
            }
            else
            {
                // Dedup chunk found
                free(tmpChunk.chunkPtr);
                tmpChunk = dataWrite_->Get_Chunk_MetaInfo(findRes);
                tmpChunkid = findRes;
                PrevDedupChunkid = findRes;
                DedupReduct += tmpChunk.chunkSize;
            }
            if (tmpChunk.HeaderFlag == 0)
                dataWrite_->Recipe_Insert(tmpChunk.chunkID);
            else
                dataWrite_->Recipe_Header_Insert(tmpChunk.chunkID);
            logicalchunkNum++;
            logicalchunkSize += tmpChunk.chunkSize;
        }
    }
    recieveQueue->done_ = false;
    return;
}

Chunk_t TreeCache2::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, uint64_t HitSF, SuperFeatures sfs)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;
    uint64_t feature_hash = HitSF; // 获取SF哈希作为Feature ID

    Chunk_t basechunk = dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    bool is_hit = chunk_cache_.contains(BasechunkId);

    // 更新Feature统计信息
    update_feature_stats(feature_hash, is_hit);

    // 决策：是否应该缓存这个Feature
    if (should_cache_feature(feature_hash))
    {
        load_feature_tree_to_cache(BasechunkId, feature_hash);
    }

    if (basechunk.basechunkID < 0)
    {
        SetTime(startIO);
        basechunk = xd3_recursive_restore_BL_time(BasechunkId); // 使用缓存恢复逻辑
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
    }
    else
    {
        basechunk = xd3_recursive_restore_BL_time(BasechunkId); // 使用缓存恢复逻辑
        if (basechunk.FirstChildID < 0)                         // if only one layer
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

        // --- 优化开始: 遍历子节点和兄弟节点，并检查缓存 ---
        int current_node_id = resultchunk.FirstChildID;
        while (current_node_id >= 0)
        {
            uint8_t *basechunk_ptr = nullptr;
            size_t current_basechunk_size = 0;
            std::vector<uint8_t> cachedData;

            cache2AccessCount++;
            if (chunk_cache_.tryGet(current_node_id, cachedData))
            {
                // 缓存命中
                cache2HitCount++;
                current_basechunk_size = cachedData.size();
                basechunk_ptr = (uint8_t *)malloc(current_basechunk_size);
                memcpy(basechunk_ptr, cachedData.data(), current_basechunk_size);
            }
            else
            {
                // 缓存未命中，执行解压
                SetTime(startIO);
                Chunk_t TmpNodeChunk = dataWrite_->Get_Chunk_Info(current_node_id);
                SetTime(endIO);
                SetTime(startIO, endIO, IOTime);

                // CombinedBuffer 存储的是父节点的内容
                basechunk_ptr = xd3_decode(TmpNodeChunk.chunkPtr, TmpNodeChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &current_basechunk_size);

                if (TmpNodeChunk.loadFromDisk)
                {
                    free(TmpNodeChunk.chunkPtr);
                }
            }

            // 使用恢复的 basechunk_ptr 进行比较
            if (basechunk_ptr != nullptr && current_basechunk_size > 0)
            {
                xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk_ptr, current_basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer);
                if (tmpsaveSize < resultchunk.saveSize)
                {
                    Chunk_t node_meta = dataWrite_->Get_Chunk_MetaInfo(current_node_id);
                    resultchunk.saveSize = tmpsaveSize;
                    resultchunk.chunkID = node_meta.chunkID;
                    resultchunk.chunkSize = node_meta.chunkSize;
                    resultchunk.FirstChildID = node_meta.FirstChildID;
                    memcpy(MinBaseBuffer, basechunk_ptr, node_meta.chunkSize);
                }
            }

            if (basechunk_ptr != nullptr)
            {
                free(basechunk_ptr);
            }

            // 移动到下一个兄弟节点
            current_node_id = dataWrite_->Get_Chunk_MetaInfo(current_node_id).FirstBroID;
        }
        StatsFit(tmpFatherID, resultchunk.chunkID, sfs);
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

// ... 您的 xd3_encode_buffer 和 StatsFit 现有代码 ...
uint8_t *TreeCache2::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void TreeCache2::StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs)
{
    if (dataWrite_->chunklist[FatherID].BeforeFit == FitID)
    {
        dataWrite_->chunklist[FatherID].FitCount++;
    }
    else
    {
        dataWrite_->chunklist[FatherID].BeforeFit = FitID;
        dataWrite_->chunklist[FatherID].FitCount = 1;
    }
    if (dataWrite_->chunklist[FatherID].FitCount > 4)
    {
        if (table.Tree_SF_Find(sfs) == FatherID)
            table.Tree_SF_ReWrite(sfs, FitID);
    }
}

// =================== FI-Cache (频率简化版) 核心实现 =====================

Chunk_t TreeCache2::xd3_recursive_restore_BL_time(uint64_t BasechunkId)
{
    std::vector<uint8_t> cachedData;
    cache2AccessCount++;
    if (chunk_cache_.tryGet(BasechunkId, cachedData))
    {
        cache2HitCount++;
        Chunk_t cachedChunk;
        cachedChunk.chunkID = BasechunkId;
        cachedChunk.chunkSize = cachedData.size();
        cachedChunk.chunkPtr = (uint8_t *)malloc(cachedData.size());
        memcpy(cachedChunk.chunkPtr, cachedData.data(), cachedData.size());
        cachedChunk.loadFromDisk = false;
        return cachedChunk;
    }

    // Cache Miss: 按需解压
    Chunk_t restored_chunk = decompress_on_demand(BasechunkId);
    return restored_chunk;
}

void TreeCache2::update_feature_stats(uint64_t feature_hash, bool hit)
{
    if (feature_stats_.find(feature_hash) == feature_stats_.end())
    {
        feature_stats_.emplace(feature_hash, FeatureStats(feature_hash));
    }
    feature_stats_.at(feature_hash).record_access();
}

bool TreeCache2::should_cache_feature(uint64_t feature_hash)
{
    if (feature_stats_.find(feature_hash) == feature_stats_.end())
    {
        return false;
    }
    // 仅当访问频率超过阈值时，才认为值得缓存
    return feature_stats_.at(feature_hash).calculate_importance() > IMPORTANCE_THRESHOLD;
}

void TreeCache2::load_feature_tree_to_cache(uint64_t root_chunk_id, uint64_t feature_hash)
{
    if (cached_features_.count(feature_hash))
        return; // 已在缓存中

    // 1. 收集此Feature树的所有chunk ID (简化为根节点和第一层孩子)
    vector<uint64_t> tree_chunks;
    tree_chunks.push_back(root_chunk_id);
    Chunk_t root_meta = dataWrite_->Get_Chunk_MetaInfo(root_chunk_id);
    int child_id = root_meta.FirstChildID;
    while (child_id >= 0)
    {
        tree_chunks.push_back(child_id);
        Chunk_t child_meta = dataWrite_->Get_Chunk_MetaInfo(child_id);
        child_id = child_meta.FirstBroID;
    }
    feature_tree_chunks_[feature_hash] = tree_chunks;

    // 2. 检查空间是否足够，不够则淘汰
    if (chunk_cache_.size() + tree_chunks.size() > CACHE_MAX_SIZE)
    {
        evict_least_important_features();
    }

    // 3. 解压并加载到缓存
    for (uint64_t chunk_id : tree_chunks)
    {
        if (!chunk_cache_.contains(chunk_id))
        {
            Chunk_t chunk_data = decompress_on_demand(chunk_id);
            if (chunk_data.chunkPtr != nullptr && chunk_data.chunkSize > 0)
            {
                chunk_cache_.insert(chunk_id, std::vector<uint8_t>(chunk_data.chunkPtr, chunk_data.chunkPtr + chunk_data.chunkSize));
            }
            free(chunk_data.chunkPtr);
        }
    }

    // 4. 更新缓存状态
    cached_features_.insert(feature_hash);
}

void TreeCache2::evict_least_important_features()
{
    if (cached_features_.empty())
        return;

    size_t target_eviction_count = chunk_cache_.size() * EVICTION_RATIO;
    size_t evicted_count = 0;

    while (evicted_count < target_eviction_count && !cached_features_.empty())
    {
        double min_importance = 1e18; // 使用一个足够大的初始值
        uint64_t least_important_fid = 0;

        // 找出已缓存特征中，访问频率最低的一个
        for (const auto &fid : cached_features_)
        {
            double importance = feature_stats_.at(fid).calculate_importance();
            if (importance < min_importance)
            {
                min_importance = importance;
                least_important_fid = fid;
            }
        }

        if (least_important_fid == 0)
            break; // 没找到可淘汰的

        // 移除该Feature的所有chunks
        if (feature_tree_chunks_.count(least_important_fid))
        {
            for (uint64_t chunk_id : feature_tree_chunks_.at(least_important_fid))
            {
                if (chunk_cache_.contains(chunk_id))
                {
                    chunk_cache_.remove(chunk_id);
                    evicted_count++;
                }
            }
            feature_tree_chunks_.erase(least_important_fid);
        }
        cached_features_.erase(least_important_fid);
    }
}

Chunk_t TreeCache2::decompress_on_demand(uint64_t chunk_id)
{
    std::vector<Chunk_t> chunkChain;
    chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(chunk_id));

    while (chunkChain.back().basechunkID >= 0)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(chunkChain.back().basechunkID));
    }

    SetTime(startIO);
    Chunk_t base_chunk = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
    SetTime(endIO);
    SetTime(startIO, endIO, IOTime);

    uint8_t *buffer = (uint8_t *)malloc(base_chunk.chunkSize);
    memcpy(buffer, base_chunk.chunkPtr, base_chunk.chunkSize);
    size_t buffer_size = base_chunk.chunkSize;

    if (base_chunk.loadFromDisk)
    {
        free(base_chunk.chunkPtr);
    }

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        SetTime(startIO);
        Chunk_t delta_chunk = dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        size_t restored_size = 0;
        uint8_t *restored_ptr = xd3_decode(delta_chunk.chunkPtr, delta_chunk.saveSize, buffer, buffer_size, &restored_size);

        if (delta_chunk.loadFromDisk)
        {
            free(delta_chunk.chunkPtr);
        }
        free(buffer);

        buffer = restored_ptr;
        buffer_size = restored_size;
    }

    Chunk_t final_chunk;
    final_chunk.chunkID = chunk_id;
    final_chunk.chunkPtr = buffer;
    final_chunk.chunkSize = buffer_size;
    final_chunk.loadFromDisk = false;
    return final_chunk;
}