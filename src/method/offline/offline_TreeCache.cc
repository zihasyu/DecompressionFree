#include "../../../include/Offline/offline_treecache.h"

OfflineTreeCache::OfflineTreeCache()
    : chunkCache(1024, 64) // Initialize cache
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    CombinedBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

OfflineTreeCache::~OfflineTreeCache()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
    free(CombinedBuffer);
}

void OfflineTreeCache::ProcessTrace()
{
    string tmpChunkContent;
    SuperFeatures superfeature;

    // [CHANGE] Get all chunks from the source dataWrite_
    vector<Chunk_t> &sourceList = dataWrite_->chunklist;
    size_t totalChunks = sourceList.size();

    // [CHANGE] Iterate through all chunks using a for loop
    for (size_t i = 0; i < totalChunks; i++)
    {
        // 1. Restore the chunk content to its original form
        Chunk_t tmpChunk = dataWrite_->Get_Chunk_MetaInfo(i);
        if (tmpChunk.basechunkID >= 0)
        {
            Chunk_t tmpPreChunk = dataWrite_->Get_Chunk_Info(tmpChunk.basechunkID);
            Chunk_t tmpDeltaChunk = dataWrite_->Get_Chunk_Info(i);
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
            Chunk_t rawChunk = dataWrite_->Get_Chunk_Info(i);
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

        // 2. Re-compute super features for the original content
        tmpChunkContent.assign((char *)tmpChunk.chunkPtr, tmpChunk.chunkSize);
        uint64_t basechunkid = -1;
        if (tmpChunk.chunkSize > 60)
        {
            startSF = std::chrono::high_resolution_clock::now();
            superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
            endSF = std::chrono::high_resolution_clock::now();
            SFTime += (endSF - startSF);
            basechunkid = table.Tree_SF_Find(superfeature);
        }

        // 3. Re-process the chunk
        if (basechunkid != -1)
        {
            auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk, superfeature);
            uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

            if (RestoreBasechunk.loadFromDisk)
                free(RestoreBasechunk.chunkPtr);

            if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
            {
                // Delta failed, fallback to LZ4
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

                if (tmpChunk.chunkSize > 60)
                    table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
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
                // Delta succeeded
                tmpChunk.deltaFlag = DELTA;
                tmpChunk.basechunkID = RestoreBasechunk.chunkID;

                if (tmpChunk.chunkSize > 60)
                    table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);

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
            // No base found, treat as a new base chunk
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

            if (tmpChunk.chunkSize > 60)
                table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
            basechunkNum++;
            basechunkSize += tmpChunk.saveSize;

            if (tmpChunk.deltaFlag == NO_LZ4)
                offline_dataWrite_->Chunk_Insert(tmpChunk);
            else
                offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
        }

        // Update statistics
        uniquechunkNum++;
        uniquechunkSize += tmpChunk.saveSize;
        logicalchunkNum++;
        logicalchunkSize += tmpChunk.chunkSize;
    }

    // Finalize
    cout << "Version " << ads_Version
         << " Cache Stats - Hits: " << cacheHitCount
         << " Accesses: " << cacheAccessCount
         << " Hit Rate: " << (cacheAccessCount > 0 ? (float)cacheHitCount / cacheAccessCount * 100 : 0) << "%"
         << endl;
    ads_Version++;
    SFnum = basechunkNum * 3;
    return;
}

Chunk_t OfflineTreeCache::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;

    // [CHANGE] Get base chunk info from the *destination* offline_dataWrite_
    Chunk_t basechunk = offline_dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    if (basechunk.basechunkID < 0)
    {
        basechunk = offline_dataWrite_->Get_Chunk_Info(BasechunkId);
        if (basechunk.FirstChildID < 0)
            return basechunk;
    }
    else
    {
        // Restore recursively from the destination tree
        basechunk = xd3_recursive_restore_offline_time(BasechunkId);
        if (basechunk.FirstChildID < 0)
            return basechunk;
    }

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
        uint64_t tmpFatherID = resultchunk.chunkID;
        uint64_t tmpChildID = resultchunk.chunkID;

        Chunk_t TmpChildChunk = offline_dataWrite_->Get_Chunk_Info(resultchunk.FirstChildID);
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
            free(TmpChildChunk.chunkPtr);
        free(basechunk_ptr);

        Chunk_t TmpBroChunk = TmpChildChunk;
        while (TmpBroChunk.FirstBroID >= 0)
        {
            TmpBroChunk = offline_dataWrite_->Get_Chunk_Info(TmpBroChunk.FirstBroID);
            uint8_t *basechunk_ptr = xd3_decode(TmpBroChunk.chunkPtr, TmpBroChunk.saveSize, CombinedBuffer, basechunk.chunkSize, &basechunk_size);
            xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer);
            if (tmpsaveSize < resultchunk.saveSize)
            {
                resultchunk.saveSize = tmpsaveSize;
                resultchunk.chunkID = TmpBroChunk.chunkID;
                resultchunk.chunkSize = TmpBroChunk.chunkSize;
                resultchunk.FirstChildID = TmpBroChunk.FirstChildID;
                memcpy(MinBaseBuffer, basechunk_ptr, TmpBroChunk.chunkSize);
            }
            if (TmpBroChunk.loadFromDisk)
                free(TmpBroChunk.chunkPtr);
            free(basechunk_ptr);
        }

        StatsHit(tmpFatherID, resultchunk.chunkID, sfs);

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

uint8_t *OfflineTreeCache::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
{
    SetTime(startMiEncode);
    size_t deltachunkSize;
    int ret = xd3_encode_memory(targetChunkbuffer, targetChunkbuffer_size, baseChunkBuffer, baseChunkBuffer_size, tmpbuffer, &deltachunkSize, CONTAINER_MAX_SIZE * 2, 0);
    if (ret != 0)
    {
        cout << "delta error: " << xd3_strerror(ret) << endl;
    }
    *deltaChunkBuffer_size = (deltachunkSize > 0) ? deltachunkSize : INT_MAX;
    memcpy(tmpDeltaBuffer, tmpbuffer, deltachunkSize);
    SetTime(endMiEncode);
    SetTime(startMiEncode, endMiEncode, EncodeTime);
    return tmpDeltaBuffer;
}

void OfflineTreeCache::StatsHit(uint64_t FatherID, uint64_t HitID, SuperFeatures sfs)
{
    // [CHANGE] StatsHit should operate on the destination data (offline_dataWrite_)
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
        if (table.Tree_SF_Find(sfs) == FatherID)
            table.Tree_SF_ReWrite(sfs, HitID);
    }
}

// This function restores from the SOURCE dataWrite_ and uses the cache
Chunk_t OfflineTreeCache::xd3_recursive_restore_BL_time(uint64_t BasechunkId)
{
    std::vector<uint8_t> cachedData;
    cacheAccessCount++;
    if (chunkCache.tryGet(BasechunkId, cachedData))
    {
        cacheHitCount++;
        Chunk_t cachedChunk;
        cachedChunk.chunkID = BasechunkId;
        cachedChunk.chunkSize = cachedData.size();
        cachedChunk.chunkPtr = (uint8_t *)malloc(cachedData.size());
        cachedChunk.FirstChildID = dataWrite_->chunklist[BasechunkId].FirstChildID;
        memcpy(cachedChunk.chunkPtr, cachedData.data(), cachedData.size());
        cachedChunk.loadFromDisk = true; // Mark for freeing
        return cachedChunk;
    }

    chunkHotMap[BasechunkId]++;

    std::vector<Chunk_t> chunkChain;
    Chunk_t current = dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    chunkChain.push_back(current);

    while (current.basechunkID >= 0)
    {
        current = dataWrite_->Get_Chunk_MetaInfo(current.basechunkID);
        chunkChain.push_back(current);
    }

    Chunk_t basechunk = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        Chunk_t deltaChunk = dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        size_t restored_size = 0;
        uint8_t *restored_ptr = xd3_decode(deltaChunk.chunkPtr, deltaChunk.saveSize, basechunk.chunkPtr, basechunk.chunkSize, &restored_size);

        if (basechunk.loadFromDisk)
            free(basechunk.chunkPtr);
        if (deltaChunk.loadFromDisk)
            free(deltaChunk.chunkPtr);

        basechunk.chunkPtr = restored_ptr;
        basechunk.chunkSize = restored_size;
        basechunk.loadFromDisk = true; // The new base is malloc'd
    }

    int hotThreshold = 2;
    if (dataWrite_->chunklist[BasechunkId].basechunkID > 0 && chunkHotMap[BasechunkId] >= hotThreshold)
    {
        chunkCache.insert(BasechunkId, std::vector<uint8_t>(basechunk.chunkPtr, basechunk.chunkPtr + basechunk.chunkSize));
    }

    basechunk.chunkID = BasechunkId; // Ensure final chunk has the correct ID
    return basechunk;
}

// [NEW] This function restores from the DESTINATION offline_dataWrite_ and does NOT use the cache
Chunk_t OfflineTreeCache::xd3_recursive_restore_offline_time(uint64_t BasechunkId)
{
    std::vector<Chunk_t> chunkChain;
    Chunk_t current = offline_dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    chunkChain.push_back(current);

    while (current.basechunkID >= 0)
    {
        current = offline_dataWrite_->Get_Chunk_MetaInfo(current.basechunkID);
        chunkChain.push_back(current);
    }

    Chunk_t basechunk = offline_dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        Chunk_t deltaChunk = offline_dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        size_t restored_size = 0;
        uint8_t *restored_ptr = xd3_decode(deltaChunk.chunkPtr, deltaChunk.saveSize, basechunk.chunkPtr, basechunk.chunkSize, &restored_size);

        if (basechunk.loadFromDisk)
            free(basechunk.chunkPtr);
        if (deltaChunk.loadFromDisk)
            free(deltaChunk.chunkPtr);

        basechunk.chunkPtr = restored_ptr;
        basechunk.chunkSize = restored_size;
        basechunk.loadFromDisk = true;
    }

    basechunk.chunkID = BasechunkId;
    return basechunk;
}
