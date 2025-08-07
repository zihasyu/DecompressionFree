#include "../../include/AllGreedy.h"

AllGreedy::AllGreedy()
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

AllGreedy::~AllGreedy()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
}

void AllGreedy::ProcessTrace()
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

                    basechunkid = table.Tree_SF_Find(superfeature);
                    // auto ret = table.GetSimilarRecordsKeys(tmpChunkHash);
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = FindBest(basechunkid, tmpChunk);
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

Chunk_t AllGreedy::FindBest(uint64_t BasechunkId, const Chunk_t &Targetchunk)
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
    }
    else
    {
        basechunk = xd3_recursive_restore_BL_time(BasechunkId);
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
    }
    memcpy(MinBaseBuffer, basechunk.chunkPtr, basechunk.chunkSize);
    resultchunk.saveSize = INT_MAX;
    resultchunk.chunkSize = basechunk.chunkSize;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = basechunk.chunkID;
    resultchunk.FirstChildID = basechunk.FirstChildID;

    // xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk.chunkPtr, basechunk.chunkSize, &resultchunk.saveSize, deltaMaxChunkBuffer);

    if (basechunk.loadFromDisk)
        free(basechunk.chunkPtr); // free base chunk memory

    std::queue<uint64_t> toVisit;
    toVisit.push(BasechunkId);

    while (!toVisit.empty())
    {
        uint64_t currentID = toVisit.front();
        toVisit.pop();

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
            resultchunk.FirstChildID = current.FirstChildID;
            memcpy(MinBaseBuffer, current.chunkPtr, current.chunkSize);
        }

        for (uint64_t childID = current.FirstChildID; childID != -1; childID = dataWrite_->chunklist[childID].FirstBroID)
        {
            toVisit.push(childID);
        }
        if (current.loadFromDisk)
            free(current.chunkPtr); // free current chunk memory, but if it in pool or memory container, it will not be freed
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    return resultchunk;
}

uint8_t *AllGreedy::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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