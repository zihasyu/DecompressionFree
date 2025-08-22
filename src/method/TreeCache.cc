#include "../../include/Tree/TreeCache.h"

TreeCache::TreeCache()
    : chunkCache(1024, 64)
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

TreeCache::~TreeCache()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
}

void TreeCache::ProcessTrace()
{
    string tmpChunkHash;
    string tmpChunkContent;
    SuperFeatures superfeature;
    while (true)
    {
        string hashStr;
        hashStr.assign(CHUNK_HASH_SIZE, 0);
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            cout << "Version " << ads_Version
            << " Cache Stats - Hits: " << cacheHitCount
            << " Accesses: " << cacheAccessCount 
            << " Hit Rate: " << (float)cacheHitCount/cacheAccessCount*100 << "%" 
            << endl;

            //chunkCache.clear();

            cacheHitCount = 0;
            cacheAccessCount = 0;

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

                    basechunkid = table.Tree_SF_Find(superfeature);
                    // auto ret = table.GetSimilarRecordsKeys(tmpChunkHash);
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk, superfeature);
                    uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);
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

Chunk_t TreeCache::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;

    Chunk_t basechunk = dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    if (basechunk.basechunkID < 0)
    {
        SetTime(startIO);
        basechunk = dataWrite_->Get_Chunk_Info(BasechunkId);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
        // basechunk = xd3_recursive_restore_BL_time(BasechunkId);
    }
    else
    {
        basechunk = xd3_recursive_restore_BL_time(BasechunkId);
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
        Chunk_t TmpChildChunk = dataWrite_->Get_Chunk_Info(resultchunk.FirstChildID);
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
            TmpBroChunk = dataWrite_->Get_Chunk_Info(TmpBroChunk.FirstBroID);
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

uint8_t *TreeCache::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void TreeCache::StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs)
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
        table.Tree_SF_ReWrite(sfs, FitID);
    }
}

Chunk_t TreeCache::xd3_recursive_restore_BL_time(uint64_t BasechunkId)
{
    std::vector<uint8_t> cachedData;
    cacheAccessCount++;
    if(chunkCache.tryGet(BasechunkId, cachedData)){
        cacheHitCount++;
        Chunk_t cachedChunk;
        cachedChunk.chunkID = BasechunkId;
        cachedChunk.chunkSize = cachedData.size();
        cachedChunk.chunkPtr = (uint8_t*)malloc(cachedData.size());
        cachedChunk.FirstChildID = dataWrite_->chunklist[BasechunkId].FirstChildID;
        memcpy(cachedChunk.chunkPtr, cachedData.data(), cachedData.size());
        cachedChunk.loadFromDisk = false;
        return cachedChunk;
    }

    chunkHotMap[BasechunkId]++;

    // SetTime(startMiDelta);
    std::vector<Chunk_t> chunkChain;
    Chunk_t basechunk;
    size_t basechunk_size = 0;
    chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(BasechunkId));
    // if only one layer
    if (chunkChain.back().basechunkID < 0)
    {
        SetTime(startIO);
        chunkChain.back() = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        return chunkChain.back();
    }

    // collect all delta chain blocks
    while (chunkChain.back().basechunkID >= 0)
        chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(chunkChain.back().basechunkID));
    // push the last chunk
    SetTime(startIO);
    chunkChain.back() = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
    SetTime(endIO);
    SetTime(startIO, endIO, IOTime);

    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    basechunk.loadFromDisk = false;
    basechunk.chunkSize = chunkChain.back().chunkSize;
    basechunk.chunkPtr = CombinedBuffer;
    basechunk.chunkID = chunkChain.back().chunkID;
    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr); // free base chunk memory

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        SetTime(startIO);
        chunkChain[i] = dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        uint8_t *basechunk_ptr = xd3_decode(chunkChain[i].chunkPtr, chunkChain[i].saveSize,
                                            basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);

        if (chunkChain[i].chunkSize != basechunk_size)
        {
            cout << "xd3 recursive restore error, chunk size mismatch" << endl;
            cout << "id " << chunkChain[i].chunkID << " chunkChain[i].chunkSize : " << chunkChain[i].chunkSize << "chunkChain[i].saveSize: " << chunkChain[i].saveSize
                 << " basechunksize " << basechunk.chunkSize << " restore basechunk_size : " << basechunk_size << endl;
            basechunk.chunkSize = 0;
            return basechunk;
        }
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        memcpy(CombinedBuffer, basechunk_ptr, basechunk_size);
        basechunk.chunkSize = chunkChain[i].chunkSize; // update size
        basechunk.FirstChildID = chunkChain[i].FirstChildID;
        basechunk.chunkID = chunkChain[i].chunkID;
        free(basechunk_ptr);

        basechunk_size = 0;
    }

    // SetTime(endMiDelta);
    // MiDeltaTime += endMiDelta - startMiDelta;
    int hotThreshold = 2;
    if(dataWrite_->chunklist[BasechunkId].basechunkID > 0 && chunkHotMap[BasechunkId] >= hotThreshold)
        chunkCache.insert(BasechunkId, std::vector<uint8_t>(basechunk.chunkPtr, basechunk.chunkPtr + basechunk.chunkSize));

    return basechunk;  
}