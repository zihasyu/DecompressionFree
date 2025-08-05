#include "../../include/Tree/TreeGreedy.h"

TreeGreedy::TreeGreedy()
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

TreeGreedy::~TreeGreedy()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
}

void TreeGreedy::ProcessTrace()
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
                // TreeGreedy get superfeature & get time
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
                    auto RestoreBasechunk = xd3_recursive_restore(basechunkid, tmpChunk);
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
                        tmpChunk.basechunkID = RestoreBasechunk.chunkID;
                        // cout << "tmpChunk.savesize is " << tmpChunk.saveSize << endl;
                        if (tmpChunk.chunkSize > 60)
                            table.SF_Insert(superfeature, tmpChunk.chunkID);
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

Chunk_t TreeGreedy::xd3_recursive_restore(uint64_t BasechunkId, const Chunk_t Targetchunk)
{
    SetTime(startMiDelta);
    std::vector<Chunk_t> chunkChain;
    Chunk_t basechunk;
    Chunk_t resultchunk;
    size_t basechunk_size = 0;
    SetTime(startIO);
    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));
    SetTime(endIO);
    SetTime(endIO, startIO, IOTime);
    if (chunkChain.back().basechunkID < 0) // if only one layer
        return chunkChain.back();

    while (chunkChain.back().basechunkID > 0)
    {
        SetTime(startIO);
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID));
        SetTime(endIO);
        IOTime += endIO - startIO;
    }

    // first layer init
    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    basechunk.chunkSize = chunkChain.back().chunkSize;
    basechunk.chunkPtr = CombinedBuffer;
    // greed init
    memcpy(MinBaseBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    resultchunk.chunkSize = chunkChain.back().chunkSize;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = chunkChain.back().chunkID;
    xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, chunkChain.back().chunkPtr, chunkChain.back().chunkSize, &resultchunk.saveSize, deltaMaxChunkBuffer); //*** resultchunk.saveSize save tmpMinDeltaSize only here

    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr); // free base chunk memory

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        SetTime(startDecode);
        uint8_t *basechunk_ptr = xd3_decode(chunkChain[i].chunkPtr, chunkChain[i].saveSize,
                                            basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);
        SetTime(endDecode);
        SetTime(endDecode, startDecode, DecodeTime);
        if (chunkChain[i].chunkSize != basechunk_size) // bug
        {
            cout << "xd3 recursive restore error, chunk size mismatch" << endl;
            cout << "id " << chunkChain[i].chunkID << " chunkChain[i].chunkSize : " << chunkChain[i].chunkSize << "chunkChain[i].saveSize: " << chunkChain[i].saveSize
                 << " basechunksize " << basechunk.chunkSize << " restore basechunk_size : " << basechunk_size << endl;
            basechunk.chunkSize = 0;
            if (chunkChain[i].loadFromDisk)
                free(chunkChain[i].chunkPtr);
            free(basechunk_ptr);
            return basechunk;
        }
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        // greedy updata
        uint64_t tmpsaveSize = 0;
        xd3_encode_buffer(Targetchunk.chunkPtr, Targetchunk.chunkSize, basechunk_ptr, basechunk_size, &tmpsaveSize, deltaMaxChunkBuffer);
        if (tmpsaveSize < resultchunk.saveSize)
        {
            resultchunk.saveSize = tmpsaveSize;
            resultchunk.chunkID = chunkChain[i].chunkID;
            resultchunk.chunkSize = chunkChain[i].chunkSize;
            memcpy(MinBaseBuffer, basechunk_ptr, basechunk_size);
        }
        // updata basechunk info
        memcpy(CombinedBuffer, basechunk_ptr, basechunk_size);
        basechunk.chunkSize = chunkChain[i].chunkSize;
        free(basechunk_ptr);

        basechunk_size = 0;
    }
    SetTime(endMiDelta);
    SetTime(endMiDelta, startMiDelta, MiDeltaTime);
    return resultchunk;
}

uint8_t *TreeGreedy::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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
    SetTime(endMiEncode, startMiEncode, EncodeTime);
    return tmpDeltaBuffer;
}