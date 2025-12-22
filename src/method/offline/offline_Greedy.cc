#include "../../../include/Offline/offline_greedy.h"

OfflineAllGreedy::OfflineAllGreedy()
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    CombinedBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

OfflineAllGreedy::~OfflineAllGreedy()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
    free(CombinedBuffer);
}

void OfflineAllGreedy::ProcessTrace()
{
    string tmpChunkContent;
    SuperFeatures superfeature;

    vector<Chunk_t> &sourceList = dataWrite_->chunklist;
    size_t totalChunks = sourceList.size();

    for (size_t i = 0; i < totalChunks; i++)
    {
        // 1. Restore the chunk content to its original form
        Chunk_t tmpChunkMeta = dataWrite_->Get_Chunk_MetaInfo(i);
        Chunk_t tmpChunk;
        bool needFree = false; // Flag to indicate if tmpChunk.chunkPtr needs to be freed

        if (tmpChunkMeta.deltaFlag == DELTA && tmpChunkMeta.basechunkID >= 0)
        {
            // It's a delta chunk, restore it recursively
            tmpChunk = xd3_recursive_restore_BL_time(i);
            needFree = true; // Restored chunk always needs to be freed
        }
        else
        {
            // It's a base chunk (raw or lz4), just get its content
            tmpChunk = dataWrite_->Get_Chunk_Info(i);
            needFree = tmpChunk.loadFromDisk; // Free only if it was loaded from disk
        }

        // Keep original chunkID
        tmpChunk.chunkID = tmpChunkMeta.chunkID;

        // 2. Re-compute super features for the original content
        tmpChunkContent.assign((char *)tmpChunk.chunkPtr, tmpChunk.chunkSize);
        uint64_t basechunkid = -1;
        if (tmpChunk.chunkSize > 60)
        {
            startSF = std::chrono::high_resolution_clock::now();
            superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
            endSF = std::chrono::high_resolution_clock::now();
            SFTime += (endSF - startSF);

            // Find potential base chunks using the new feature index
            basechunkid = table.SF_Find(superfeature);
        }

        // 3. Re-process the chunk (find best base and re-compress)
        if (basechunkid != -1)
        {
            // Unique chunk found
            tmpChunk.chunkID = uniquechunkNum;
            tmpChunk.deltaFlag = NO_DELTA;
            tmpChunkContent.assign((char *)tmpChunk.chunkPtr, tmpChunk.chunkSize);
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

            // 3. Re-process the chunk (find best base and re-compress)
            if (basechunkid != -1)
            // A potential base chunk was found
            {
                // Find the best base chunk among candidates
                auto RestoreBasechunk = FindBest(superfeature, tmpChunk);
                uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

                if (RestoreBasechunk.loadFromDisk)
                    free(RestoreBasechunk.chunkPtr);

                if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
                {
                    // Delta compression is not effective, fallback to LZ4
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
                        table.SF_Insert(superfeature, tmpChunk.chunkID);
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
                    // Delta compression is successful
                    tmpChunk.deltaFlag = DELTA;
                    tmpChunk.basechunkID = RestoreBasechunk.chunkID;

                    if (tmpChunk.chunkSize > 60)
                        table.SF_Insert(superfeature, tmpChunk.chunkID);

                    memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                    StatsDelta(tmpChunk);
                    free(deltachunk);

                    // Insert into the destination offline_dataWrite_
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                }
            }
            // unique chunk & base chunk
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

                if (tmpChunk.chunkSize > 60)
                    table.SF_Insert(superfeature, tmpChunk.chunkID);
                basechunkNum++;
                basechunkSize += tmpChunk.saveSize;

                // Insert into the destination offline_dataWrite_
                if (tmpChunk.deltaFlag == NO_LZ4)
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                else
                    offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
            }
        }
        if (needFree && tmpChunk.chunkPtr != nullptr)
        {
            free(tmpChunk.chunkPtr);
            tmpChunk.chunkPtr = nullptr;
        }
        // Update statistics
        uniquechunkNum++; // In offline mode, every chunk is processed as "unique"
        uniquechunkSize += tmpChunk.saveSize;
        logicalchunkNum++;
        logicalchunkSize += tmpChunk.chunkSize;
    }
    return;
}

Chunk_t OfflineAllGreedy::FindBest(SuperFeatures SF, const Chunk_t &Targetchunk)
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
        // [CHANGE] Get chunks from the *destination* offline_dataWrite_
        // because that's where the new base chunks are being stored
        Chunk_t current = xd3_recursive_restore_offline_time(currentID);
        size_t deltaSize = 0;
        xd3_encode_buffer(
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
            free(current.chunkPtr);
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    return resultchunk;
}

uint8_t *OfflineAllGreedy::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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