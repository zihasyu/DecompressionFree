#include "../../include/finesse.h"

Finesse::Finesse()
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    tmpChunkSF = (uint8_t *)malloc(FINESSE_SF_NUM * CHUNK_HASH_SIZE);
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
}

Finesse::~Finesse()
{
    free(lz4ChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(deltaMaxChunkBuffer);
    free(tmpChunkSF);
    delete[] SFindex;
}

void Finesse::ProcessTrace()
{

    while (true)
    {
        string hashStr;
        hashStr.assign(CHUNK_HASH_SIZE, 0);
        std::chrono::time_point<std::chrono::high_resolution_clock> startTime, endTime;

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
            // calculate feature
            // compute cutPoint
            // generate chunk
            GenerateHash(mdCtx, tmpChunk.chunkPtr, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, 32);
            int findRes = FP_Find(hashStr);
            if (findRes == -1)
            {
                // Unique chunk
                tmpChunk.chunkID = uniquechunkNum;
                tmpChunk.deltaFlag = NO_DELTA;
                FP_Insert(hashStr, tmpChunk.chunkID);

                // find basechunk
                startSF = std::chrono::high_resolution_clock::now();
                GetSF(tmpChunk.chunkPtr, mdCtx, tmpChunkSF, tmpChunk.chunkSize);
                endSF = std::chrono::high_resolution_clock::now();
                SFTime += (endSF - startSF);
                int basechunkID = SF_Find((char *)tmpChunkSF, FINESSE_SF_NUM * CHUNK_HASH_SIZE);
                computeSFtimes++;
                if (basechunkID == -1)
                {
                    int tmpChunkLz4CompressSize = 0;
                    startTime = std::chrono::high_resolution_clock::now();
                    tmpChunkLz4CompressSize = LZ4_compress_fast((char *)tmpChunk.chunkPtr, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                    endTime = std::chrono::high_resolution_clock::now();
                    lz4CompressionTime += (endTime - startTime);
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
                    // cout << "lz4 chunk size is " << tmpChunk.chunksize << "save size is " << tmpChunk.savesize << endl;

                    tmpChunk.basechunkID = -1;
                    SF_Insert((char *)tmpChunkSF, FINESSE_SF_NUM * CHUNK_HASH_SIZE, tmpChunk.chunkID);
                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;

                    if (tmpChunk.deltaFlag == NO_LZ4)
                        // base chunk & Lz4 error
                        dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        // base chunk &lz4 compress
                        dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                    // Reduct total
                    LocalReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
                }
                else
                {
                    Chunk_t basechunkinfo;
                    uint8_t *deltachunk;
                    int lz4size = 0;
                    tmpChunk.saveSize = 0;
                    basechunkinfo = dataWrite_->Get_Chunk_Info(basechunkID);
                    startTime = std::chrono::high_resolution_clock::now();
                    deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, basechunkinfo.chunkPtr, basechunkinfo.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);
                    endTime = std::chrono::high_resolution_clock::now();
                    deltaCompressionTime += (endTime - startTime);
                    if (tmpChunk.saveSize > tmpChunk.chunkSize)
                    {
                        cout << "bug" << endl;
                        bugCount++;
                    }
                    if (tmpChunk.saveSize == 0)
                    {
                        cout << "delta error and can't to restore" << endl;
                        return;
                    }
                    else
                    {
                        memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                        tmpChunk.deltaFlag = FINESSE_DELTA;
                        tmpChunk.basechunkID = basechunkID;
                        StatsDelta(tmpChunk);
                        free(deltachunk);
                    }

                    // free outside of Get_Chunk_Info
                    if (basechunkinfo.loadFromDisk)
                        free(basechunkinfo.chunkPtr);
                    dataWrite_->Chunk_Insert(tmpChunk);
                }

                uniquechunkNum++;
                uniquechunkSize += tmpChunk.saveSize;
            }
            else
            {
                free(tmpChunk.chunkPtr);
                tmpChunk = dataWrite_->Get_Chunk_MetaInfo(findRes);
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
