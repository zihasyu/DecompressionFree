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
    DumpReversePosStats("reverse_pos_stats.txt"); // insight2
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

                    basechunkid = table.SF_Find(superfeature);
                    // auto ret = table.GetSimilarRecordsKeys(tmpChunkHash);
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = FindBest(superfeature, tmpChunk);
                    // ForTest
                    // uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

                    if (RestoreBasechunk.loadFromDisk)
                        free(RestoreBasechunk.chunkPtr);

                    // if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)// ForTest
                    if (1)
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

                        tmpChunk.basechunkID = RestoreBasechunk.chunkID; // ForTest
                        tmpChunkid = tmpChunk.chunkID;
                        if (tmpChunk.chunkSize > 60)
                            table.SF_Insert(superfeature, tmpChunk.chunkID);
                        basechunkNum++;
                        basechunkSize += tmpChunk.saveSize;
                        LocalReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
                        // ForTest
                        // free(deltachunk);
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
                        if (tmpChunk.chunkSize > 60)
                            table.SF_Insert(superfeature, tmpChunk.chunkID);

                        // memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);// ForTest
                        StatsDelta(tmpChunk);
                        // free(deltachunk);// ForTest

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

Chunk_t AllGreedy::FindBest(SuperFeatures SF, const Chunk_t &Targetchunk)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;

    resultchunk.saveSize = INT_MAX + 1;
    resultchunk.chunkSize = 0;
    resultchunk.chunkPtr = MinBaseBuffer;
    resultchunk.loadFromDisk = false;
    resultchunk.chunkID = -1;
    auto toVisit = table.SF_Find_Mi(SF);
    size_t best_reverse_pos = (size_t)-1; // 用于记录最终最佳候选项的倒数位置
    const size_t n = toVisit.size();
    for (size_t idx = 0; idx < n; ++idx)
    {
        auto currentID = toVisit[idx]; // 通过索引访问元素
        // --- 修改到这里结束 ---

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

            // 找到了一个更好的候选项，记下它的倒数位置
            best_reverse_pos = (n == 0 ? 0 : (n - 1 - idx));
        }

        if (current.loadFromDisk)
            free(current.chunkPtr); // free current chunk memory, but if it in pool or memory container, it will not be freed
    }
    SetTime(endMiDelta);
    SetTime(startMiDelta, endMiDelta, MiDeltaTime);
    if (resultchunk.chunkID != -1)
    {
        // 单线程：直接更新容器
        reversePosCount_[best_reverse_pos] += 1;
        reversePosList_.push_back(best_reverse_pos);
    }
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

void AllGreedy::DumpReversePosStats(const std::string &path)
{
    // 由于是在单线程的析构函数中调用，数据是最终且固定的，无需再做快照
    if (reversePosCount_.empty())
    {
        std::cout << "Reverse position stats are empty, skipping dump.\n";
        return;
    }

    // --- 1. 写入聚合统计数据 (count 和 CDF) ---

    // 将 map 转换为 vector of pairs 以便排序
    std::vector<std::pair<size_t, uint64_t>> sorted_counts(reversePosCount_.begin(), reversePosCount_.end());
    std::sort(sorted_counts.begin(), sorted_counts.end(),
              [](const auto &a, const auto &b)
              {
                  return a.first < b.first; // 按 reverse_pos (key) 升序排序
              });

    // 使用 std::accumulate 计算总命中数，更现代化
    const uint64_t total_hits = std::accumulate(sorted_counts.begin(), sorted_counts.end(), 0ULL,
                                                [](uint64_t sum, const auto &p)
                                                {
                                                    return sum + p.second; // 累加 count (value)
                                                });

    std::ofstream cdf_file(path);
    if (!cdf_file.is_open())
    {
        std::cerr << "Error: Failed to open file for CDF stats: " << path << std::endl;
        return;
    }

    cdf_file << "#reverse_pos\tcount\tcdf\n";
    uint64_t cumulative_count = 0;
    for (const auto &pair : sorted_counts)
    {
        cumulative_count += pair.second;
        const double cdf = (total_hits == 0) ? 0.0 : static_cast<double>(cumulative_count) / total_hits;
        cdf_file << pair.first << "\t" << pair.second << "\t" << cdf << "\n";
    }
    cdf_file.close();

    // --- 2. 写入原始命中列表 (用于 ECDF) ---

    const std::string list_path = path + ".list";
    std::ofstream list_file(list_path);
    if (!list_file.is_open())
    {
        std::cerr << "Error: Failed to open file for raw list: " << list_path << std::endl;
        return;
    }

    for (const auto pos : reversePosList_)
    {
        list_file << pos << "\n";
    }
    list_file.close();
}