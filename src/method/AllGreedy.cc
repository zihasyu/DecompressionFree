#include "../../include/AllGreedy.h"

AllGreedy::AllGreedy(int FinalVersion_)
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    FinalVersion = FinalVersion_;
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

            if (ads_Version == FinalVersion)
            {
                DumpReversePosStats("Insight2.txt");  // insight2
                DumpForwardPosStats("Insight12.txt"); // insight12
                DumpDistanceStats("Insight11.txt");   // insight11
                DumpInsight10Stats("Insight10.txt");  // insight10
                DumpSFIndexStats();                   // insight4
            }

            if (hit_consistency_denominator_ > 0)
            {
                double ratio = static_cast<double>(hit_consistency_numerator_) / hit_consistency_denominator_;
                std::cout << "Insight3 Hit Consistency Stats:"
                          << "  - Numerator: " << hit_consistency_numerator_
                          << "  - Denominator: " << hit_consistency_denominator_
                          << "  - Ratio: " << ratio << std::endl;
            }
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
    size_t best_forward_pos = (size_t)-1; // 用于记录最终最佳候选项的正数位置
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

            // 找到了一个更好的候选项，记下它的倒数和正数位置
            best_reverse_pos = (n == 0 ? 0 : (n - 1 - idx));
            best_forward_pos = idx;
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
        forwardPosCount_[best_forward_pos] += 1;
        forwardPosList_.push_back(best_forward_pos);
        if (n > 0)
        {
            auto last_element_meta = dataWrite_->Get_Chunk_MetaInfo(toVisit.back());
            int64_t distance = static_cast<int64_t>(Targetchunk.chunkID) - static_cast<int64_t>(last_element_meta.chunkID);
            distanceList_.push_back(distance);
        }

        // --- 新增的命中一致性统计逻辑 ---
        if (n > 1) // 只有当候选列表大小 > 1 时才进行统计
        {
            // 获取候选列表中最后一个块的元数据
            auto last_element_meta = dataWrite_->Get_Chunk_MetaInfo(toVisit.back());
            // 获取它的基块 ID
            int64_t previous_hit_ID = last_element_meta.basechunkID;

            // 获取当前命中的基块 ID
            int64_t current_hit_ID = resultchunk.chunkID;

            // 增加分母
            hit_consistency_denominator_++;

            // 如果两者相同，增加分子
            if (current_hit_ID == previous_hit_ID)
            {
                hit_consistency_numerator_++;
            }
        }
        // --- Insight 10: "Skip-Delta" Analysis ---
        auto best_base_meta = dataWrite_->Get_Chunk_MetaInfo(resultchunk.chunkID);
        if (best_base_meta.basechunkID > -1)
        {
            // 1. 恢复 "祖父" 块
            Chunk_t grandparent_chunk = xd3_recursive_restore_BL_time(best_base_meta.basechunkID);

            // 2. 计算 Target 对 "祖父" 块的增量大小
            size_t grandparent_delta_size = 0;
            xd3_encode_buffer(
                Targetchunk.chunkPtr, Targetchunk.chunkSize,
                grandparent_chunk.chunkPtr, grandparent_chunk.chunkSize,
                &grandparent_delta_size, deltaMaxChunkBuffer);

            // 3. 计算差值并记录
            int64_t diff = static_cast<int64_t>(grandparent_delta_size) - static_cast<int64_t>(resultchunk.saveSize);
            insight10_diff_list_.push_back(diff);

            // 4. 释放为 "祖父" 块分配的内存
            if (grandparent_chunk.loadFromDisk)
            {
                free(grandparent_chunk.chunkPtr);
            }
        }
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

void AllGreedy::DumpSFIndexStats()
{
    std::cout << "\n--- Starting SFindex Analysis (Insight 1, 4, 5, 6) ---\n";

    // --- Insight 1: 统计 vector 大小分布 ---
    std::map<size_t, uint64_t> vector_size_counts;

    // --- Insight 4: 统计命中前半部分频率 (全局) ---
    uint64_t insight4_numerator = 0;
    uint64_t insight4_denominator = 0;

    // --- Insight 5: 按 vector 大小分组统计命中前半部分频率 ---
    std::map<size_t, std::pair<uint64_t, uint64_t>> insight5_stats;

    // --- Insight 6: 识别并记录“超大集合” ---
    const size_t threshold = static_cast<size_t>(uniquechunkNum * 3 * 0.01);
    std::cout << "Insight 6: Identifying 'Super Large Sets' with size > " << threshold
              << " (1% of ~" << uniquechunkNum * 3 << " total elements)\n";
    std::ofstream insight6_file("Insight6_super_large_sets.txt");
    if (insight6_file.is_open())
    {
        insight6_file << "# Super-large sets analysis (sets with size > " << threshold << ")\n";
        // 注意：由于 super_feature_t 是一个复杂类型，我们无法直接打印。
        // 这里使用一个自增的 set_id 作为标识符。
        insight6_file << "#set_id\tchunk_id\tbasechunk_id\n";
    }
    size_t set_id_counter = 0;

    // ===================== 核心修改 =====================
    // 直接遍历 table.SFindex 这个单一的 map，不再有外层循环。
    for (const auto &pair : table.SFindex)
    {
        // pair.first  is super_feature_t
        // pair.second is std::vector<std::size_t>
        const auto &vec = pair.second;
        const size_t vec_size = vec.size();

        // --- Insight 1 的数据收集 ---
        vector_size_counts[vec_size]++;

        // --- Insight 4 & 5 的数据收集 ---
        if (vec_size > 4)
        {
            // 使用 std::size_t 来匹配 vector 的类型
            std::unordered_set<std::size_t> first_half_ids;
            for (size_t j = 0; j < vec_size / 2; ++j)
            {
                first_half_ids.insert(vec[j]);
            }

            for (size_t j = 3; j < vec_size; ++j)
            {
                insight4_denominator++;
                insight5_stats[vec_size].second++;

                // vec 中的 ID 类型是 std::size_t
                size_t current_chunk_id = vec[j];
                auto meta = dataWrite_->Get_Chunk_MetaInfo(current_chunk_id);
                int64_t base_id = meta.basechunkID;

                // .count() 的参数需要匹配 set 的 key 类型
                if (first_half_ids.count(static_cast<std::size_t>(base_id)))
                {
                    insight4_numerator++;
                    insight5_stats[vec_size].first++;
                }
            }
        }

        // --- Insight 6 的数据收集 ---
        if (vec_size > threshold && insight6_file.is_open())
        {
            set_id_counter++; // 为这个超大集合分配一个ID
            for (size_t chunk_id : vec)
            {
                auto meta = dataWrite_->Get_Chunk_MetaInfo(chunk_id);
                insight6_file << set_id_counter << "\t" << chunk_id << "\t" << meta.basechunkID << "\n";
            }
        }
    }
    // ====================================================

    // --- 关闭 Insight 6 文件 ---
    if (insight6_file.is_open())
    {
        insight6_file.close();
        std::cout << "Insight 6: Super-large set data written to Insight6_super_large_sets.txt\n";
    }

    // --- Insight 1: 结果输出 ---
    std::ofstream insight1_file("Insight1_vector_size_cdf.txt");
    if (insight1_file.is_open())
    {
        uint64_t total_vectors = 0;
        for (const auto &pair : vector_size_counts)
            total_vectors += pair.second;

        insight1_file << "#vector_size\tcount\tcdf\n";
        uint64_t cumulative_count = 0;
        for (const auto &pair : vector_size_counts)
        {
            cumulative_count += pair.second;
            double cdf = (total_vectors == 0) ? 0.0 : static_cast<double>(cumulative_count) / total_vectors;
            insight1_file << pair.first << "\t" << pair.second << "\t" << cdf << "\n";
        }
        insight1_file.close();
        std::cout << "Insight 1: Vector size CDF data written to Insight1_vector_size_cdf.txt\n";
    }

    // --- Insight 4: 结果输出 ---
    if (insight4_denominator > 0)
    {
        double ratio = static_cast<double>(insight4_numerator) / insight4_denominator;
        std::cout << "Insight 4: Early Hit Frequency Stats (Global):\n"
                  << "  - Hits in First Half (Numerator): " << insight4_numerator << "\n"
                  << "  - Total Checks (Denominator): " << insight4_denominator << "\n"
                  << "  - Ratio: " << ratio << std::endl;
    }
    else
    {
        std::cout << "Insight 4: No data collected (denominator is zero).\n";
    }

    // --- Insight 5: 结果输出 ---
    std::ofstream insight5_file("Insight5_size_vs_early_hit.txt");
    if (insight5_file.is_open())
    {
        insight5_file << "#vector_size\tearly_hit_ratio\thits_in_first_half\ttotal_checks\n";
        for (const auto &pair : insight5_stats)
        {
            const size_t vec_size = pair.first;
            const uint64_t num = pair.second.first;
            const uint64_t den = pair.second.second;
            const double ratio = (den == 0) ? 0.0 : static_cast<double>(num) / den;
            insight5_file << vec_size << "\t" << ratio << "\t" << num << "\t" << den << "\n";
        }
        insight5_file.close();
        std::cout << "Insight 5: Size vs. Early Hit data written to Insight5_size_vs_early_hit.txt\n";
    }

    std::cout << "--- SFindex Analysis Finished ---\n";
}

void AllGreedy::DumpForwardPosStats(const std::string &path)
{
    if (forwardPosCount_.empty())
    {
        std::cout << "Forward position stats are empty, skipping dump.\n";
        return;
    }

    // --- 1. 写入聚合统计数据 (count 和 CDF) ---
    std::vector<std::pair<size_t, uint64_t>> sorted_counts(forwardPosCount_.begin(), forwardPosCount_.end());
    std::sort(sorted_counts.begin(), sorted_counts.end(),
              [](const auto &a, const auto &b)
              {
                  return a.first < b.first; // 按 forward_pos (key) 升序排序
              });

    const uint64_t total_hits = std::accumulate(sorted_counts.begin(), sorted_counts.end(), 0ULL,
                                                [](uint64_t sum, const auto &p)
                                                {
                                                    return sum + p.second;
                                                });

    std::ofstream cdf_file(path);
    if (!cdf_file.is_open())
    {
        std::cerr << "Error: Failed to open file for CDF stats: " << path << std::endl;
        return;
    }

    cdf_file << "#forward_pos\tcount\tcdf\n";
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

    for (const auto pos : forwardPosList_)
    {
        list_file << pos << "\n";
    }
    list_file.close();
}

void AllGreedy::DumpDistanceStats(const std::string &path)
{
    if (distanceList_.empty())
    {
        std::cout << "Distance stats are empty, skipping dump.\n";
        return;
    }

    // 1. 统计每个距离出现的次数
    std::map<int64_t, uint64_t> distance_counts;
    for (const auto &dist : distanceList_)
    {
        distance_counts[dist]++;
    }

    // 2. 写入聚合统计数据 (count 和 CDF)
    std::ofstream cdf_file(path);
    if (!cdf_file.is_open())
    {
        std::cerr << "Error: Failed to open file for distance stats: " << path << std::endl;
        return;
    }

    cdf_file << "#distance\tcount\tcdf\n";
    const uint64_t total_hits = distanceList_.size();
    uint64_t cumulative_count = 0;

    // std::map 的 key 是有序的，所以可以直接遍历以获得有序的 CDF
    for (const auto &pair : distance_counts)
    {
        cumulative_count += pair.second;
        const double cdf = (total_hits == 0) ? 0.0 : static_cast<double>(cumulative_count) / total_hits;
        cdf_file << pair.first << "\t" << pair.second << "\t" << cdf << "\n";
    }
    cdf_file.close();

    // 3. (可选) 写入原始距离列表
    const std::string list_path = path + ".list";
    std::ofstream list_file(list_path);
    if (!list_file.is_open())
    {
        std::cerr << "Error: Failed to open file for raw distance list: " << list_path << std::endl;
        return;
    }
    for (const auto dist : distanceList_)
    {
        list_file << dist << "\n";
    }
    list_file.close();
}

void AllGreedy::DumpInsight10Stats(const std::string &path)
{
    if (insight10_diff_list_.empty())
    {
        std::cout << "Insight 10 (Skip-Delta) stats are empty, skipping dump.\n";
        return;
    }

    // --- 版本 1: 包含所有差值 (正、负、零) 的 CDF ---

    // 1. 统计每个差值出现的次数
    std::map<int64_t, uint64_t> diff_counts;
    for (const auto &diff : insight10_diff_list_)
    {
        diff_counts[diff]++;
    }

    // 2. 写入聚合统计数据 (count 和 CDF)
    std::ofstream cdf_file(path);
    if (!cdf_file.is_open())
    {
        std::cerr << "Error: Failed to open file for Insight 10 stats: " << path << std::endl;
    }
    else
    {
        cdf_file << "#delta_size_diff\tcount\tcdf\n";
        const uint64_t total_count = insight10_diff_list_.size();
        uint64_t cumulative_count = 0;

        for (const auto &pair : diff_counts)
        {
            cumulative_count += pair.second;
            const double cdf = (total_count == 0) ? 0.0 : static_cast<double>(cumulative_count) / total_count;
            cdf_file << pair.first << "\t" << pair.second << "\t" << cdf << "\n";
        }
        cdf_file.close();
    }

    // 3. (可选) 写入原始差值列表
    const std::string list_path = path + ".list";
    std::ofstream list_file(list_path);
    if (!list_file.is_open())
    {
        std::cerr << "Error: Failed to open file for raw Insight 10 list: " << list_path << std::endl;
    }
    else
    {
        for (const auto diff : insight10_diff_list_)
        {
            list_file << diff << "\n";
        }
        list_file.close();
    }

    // --- 版本 2: 只包含正差值 (> 0) 的 CDF ---

    std::map<int64_t, uint64_t> positive_diff_counts;
    uint64_t total_positive_count = 0;
    for (const auto &diff : insight10_diff_list_)
    {
        if (diff > 0)
        {
            positive_diff_counts[diff]++;
            total_positive_count++;
        }
    }

    if (total_positive_count > 0)
    {
        const std::string positive_path = "Insight10_positive_only.txt";
        std::ofstream pos_cdf_file(positive_path);
        if (!pos_cdf_file.is_open())
        {
            std::cerr << "Error: Failed to open file for Insight 10 positive stats: " << positive_path << std::endl;
        }
        else
        {
            pos_cdf_file << "#positive_delta_size_diff\tcount\tcdf\n";
            uint64_t cumulative_positive_count = 0;
            for (const auto &pair : positive_diff_counts)
            {
                cumulative_positive_count += pair.second;
                const double cdf = static_cast<double>(cumulative_positive_count) / total_positive_count;
                pos_cdf_file << pair.first << "\t" << pair.second << "\t" << cdf << "\n";
            }
            pos_cdf_file.close();
        }
    }
}