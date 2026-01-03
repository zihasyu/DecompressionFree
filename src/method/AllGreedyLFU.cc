#include "../../include/AllGreedyLFU.h"

AllGreedyLFU::AllGreedyLFU()
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));

    chunkCache = std::make_unique<caches::fixed_sized_cache<uint64_t, std::vector<uint8_t>, caches::LFUCachePolicy>>(/*cache容量*/ 1024);
}

AllGreedyLFU::~AllGreedyLFU()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
}

void AllGreedyLFU::ProcessTrace()
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
            // 输出lfu cache命中率
            cout << "Cache Stats - Hits: " << cacheHitCount
                 << " Accesses: " << cacheAccessCount
                 << " Hit Rate: " << (cacheAccessCount > 0 ? (float)cacheHitCount / cacheAccessCount * 100 : 0) << "%" << endl;

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
                // TreeCut get superfeature & get time
                uint64_t basechunkid = -1;
                // compute SF
                if (tmpChunk.chunkSize > 60)
                {
                    startSF = std::chrono::high_resolution_clock::now();
                    superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
                    endSF = std::chrono::high_resolution_clock::now();
                    SFTime += (endSF - startSF);

                    basechunkid = table.SF_Find(superfeature);
                    // auto ret = table.GetSimilarRecordsKeys(tmpChunkHash);
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = FindBest(superfeature, tmpChunk);
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
                            table.SF_Insert(superfeature, tmpChunk.chunkID);
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
                            table.SF_Insert(superfeature, tmpChunk.chunkID);

                        memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                        StatsDelta(tmpChunk);
                        free(deltachunk);

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
                        table.SF_Insert(superfeature, tmpChunk.chunkID);
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

Chunk_t AllGreedyLFU::FindBest(SuperFeatures SF, const Chunk_t &Targetchunk)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;

    resultchunk.saveSize = INT_MAX + 1;
    resultchunk.chunkSize = 0;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = -1;
    auto toVisit = table.SF_Find_Mi(SF);

    for (auto currentID : toVisit)
    {
        Chunk_t current = xd3_recursive_restore_BL_time(currentID);
        size_t deltaSize = 0;
        uint8_t *delta = xd3_encode_buffer(
            Targetchunk.chunkPtr, Targetchunk.chunkSize,
            current.chunkPtr, current.chunkSize,
            &deltaSize, deltaMaxChunkBuffer);

        if (deltaSize < resultchunk.saveSize)
        {
            resultchunk.chunkSize = current.chunkSize;
            resultchunk.saveSize = deltaSize;
            resultchunk.chunkID = currentID;
            memcpy(MinBaseBuffer, current.chunkPtr, current.chunkSize);
        }

        if (current.loadFromDisk)
            free(current.chunkPtr); // free current chunk memory, but if it in pool or memory container, it will not be freed
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    return resultchunk;
}

uint8_t *AllGreedyLFU::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

Chunk_t AllGreedyLFU::xd3_recursive_restore_BL_time(uint64_t BasechunkId)
{
    //cacheAccessCount++;

    // 1. 构造依赖链
    std::vector<Chunk_t> chunkChain;
    chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(BasechunkId));
    while (chunkChain.back().basechunkID >= 0)
        chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(chunkChain.back().basechunkID));

    // 2. 从前往后找cache命中点
    int cacheIdx = -1;
    std::vector<uint8_t> cachedData;
    for (int i = 0; i < chunkChain.size(); ++i) {
        cacheAccessCount++;    
        auto [ptr, found] = chunkCache->TryGet(chunkChain[i].chunkID);
        if (found && ptr) {
            cacheHitCount++;
            cachedData = *ptr;
            cacheIdx = i;
            break;
        }
    }

    Chunk_t basechunk;
    size_t basechunk_size = 0;

    // 3. 如果有cache命中，从cache点恢复，否则从最底层恢复
    if (cacheIdx != -1) {
        // 用cache内容初始化到 CombinedBuffer
        memcpy(CombinedBuffer, cachedData.data(), cachedData.size());
        basechunk.chunkID = chunkChain[cacheIdx].chunkID;
        basechunk.chunkSize = cachedData.size();
        basechunk.chunkPtr = CombinedBuffer;
        basechunk.loadFromDisk = false;
        basechunk_size = cachedData.size();
    } else {
        // 最底层base chunk
        SetTime(startIO);
        chunkChain.back() = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
        basechunk.loadFromDisk = false;
        basechunk.chunkSize = chunkChain.back().chunkSize;
        basechunk.chunkPtr = CombinedBuffer;
        basechunk.chunkID = chunkChain.back().chunkID;
        basechunk_size = chunkChain.back().chunkSize;
        if (chunkChain.back().loadFromDisk)
            free(chunkChain.back().chunkPtr);
        cacheIdx = chunkChain.size() - 1;
    }

    // 4. 从cacheIdx-1往前递归恢复
    for (int i = cacheIdx - 1; i >= 0; --i) {
        SetTime(startIO);
        chunkChain[i] = dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        uint8_t *basechunk_ptr = xd3_decode(chunkChain[i].chunkPtr, chunkChain[i].saveSize,
                                            basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);

        if (chunkChain[i].chunkSize != basechunk_size) {
            cout << "xd3 recursive restore error, chunk size mismatch" << endl;
            basechunk.chunkSize = 0;
            if (basechunk_ptr) free(basechunk_ptr);
            return basechunk;
        }
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        memcpy(CombinedBuffer, basechunk_ptr, basechunk_size);
        free(basechunk_ptr);
        basechunk.chunkPtr = CombinedBuffer;
        basechunk.chunkSize = chunkChain[i].chunkSize;
        basechunk.chunkID = chunkChain[i].chunkID;
    }

    // 5. 插入cache
    chunkCache->Put(BasechunkId, std::vector<uint8_t>(basechunk.chunkPtr, basechunk.chunkPtr + basechunk.chunkSize));

    return basechunk;
}