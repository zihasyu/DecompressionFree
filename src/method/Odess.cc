#include "../../include/odess.h"

Odess::Odess()
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
}

Odess::Odess(int offlineMethod, bool promoteNewBase)
    : promoteNewBase_(promoteNewBase), offlineMethod(offlineMethod)
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    rootChunkMap = new std::unordered_map<uint64_t, std::vector<uint64_t>>();
}

Odess::~Odess()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    if (rootChunkMap != nullptr)
    {
        delete rootChunkMap;
    }
}

bool Odess::IsLosslessGroupRoot(uint64_t rootID) const
{
    // New inline chunks have not gone through delta compression yet and are
    // stored raw. Historical roots use the completed offline generation's
    // actual storage type.
    if (offlineIndexSource_ == nullptr || rootID >= offlineIndexBoundary_)
        return true;
    if (rootID >= offlineIndexSource_->chunklist.size())
        return false;
    return offlineIndexSource_->chunklist[rootID].deltaFlag != DELTA;
}

void Odess::RefreshGroupIndexCounts(dataWrite *offlineSource, uint64_t onlineChunkBoundary)
{
    GroupIndexEntryCount = 0;
    GroupIndexReferenceCount = 0;
    if (rootChunkMap != nullptr && offlineSource != nullptr)
    {
        for (const auto &[rootID, chunkIDs] : *rootChunkMap)
        {
            if (rootID < offlineSource->chunklist.size() &&
                offlineSource->chunklist[rootID].deltaFlag != DELTA)
            {
                ++GroupIndexEntryCount;
                GroupIndexReferenceCount += chunkIDs.size();
            }
        }
    }
    offlineIndexSource_ = offlineSource;
    offlineIndexBoundary_ = onlineChunkBoundary;
}

void Odess::PrintRebaseStats(std::ostream &out, uint64_t backupNumber) const
{
    out << "Rebase promotions after online backup " << backupNumber << ": "
        << rebaseReplacementCount_ << endl;
    out << "Rebase delta bytes appended: " << rebaseBytesAppended_
        << ", stale bytes retained: " << rebaseStaleBytesRetained_ << endl;
    out << "Maximum dependency chain depth: " << maxRebaseChainDepth_ << endl;
    out << "Append write time: " << rebaseAppendTime_.count() << "s" << endl;
}

void Odess::ProcessTrace()
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
            dataWrite_->versionEndPoints.push_back(uniquechunkNum);
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
                // Odess get superfeature & get time
                uint64_t basechunkid = -1;
                uint64_t treeBaseChunkid = -1;
                // compute SF
                if (tmpChunk.chunkSize > 60)
                {
                    startSF = std::chrono::high_resolution_clock::now();
                    superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
                    endSF = std::chrono::high_resolution_clock::now();
                    SFTime += (endSF - startSF);

                    if (offlineMethod >= 0)
                    {
                        treeBaseChunkid = table.Tree_SF_Find(superfeature);
                        table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                    }
                    basechunkid = table.SF_Find(superfeature);
                }

                // Rebase baseline: keep every searchable candidate lossless.
                // When B matches A, store B independently and try to replace
                // A's payload with delta(A | B), then move the feature entry
                // from A to B.  B has a larger ID, so this creates a forward
                // dependency while keeping the graph acyclic.
                if (promoteNewBase_)
                {
                    Chunk_t oldBase;
                    vector<uint8_t> oldBaseBytes;
                    uint64_t oldBaseStoredSize = 0;
                    uint64_t promotedDeltaSize = 0;
                    bool canPromote = false;
                    vector<uint8_t> promotedDelta;

                    if (basechunkid != static_cast<uint64_t>(-1))
                    {
                        oldBase = dataWrite_->Get_Chunk_Info(basechunkid);
                        if (oldBase.chunkPtr != nullptr && oldBase.chunkSize > 0)
                        {
                            oldBaseBytes.assign(oldBase.chunkPtr,
                                                oldBase.chunkPtr + oldBase.chunkSize);
                            oldBaseStoredSize = oldBase.deltaFlag == NO_LZ4
                                ? oldBase.chunkSize : oldBase.saveSize;

                            size_t encodedSize = 0;
                            const int encodeResult = xd3_encode_memory(
                                oldBaseBytes.data(), oldBaseBytes.size(),
                                tmpChunk.chunkPtr, tmpChunk.chunkSize,
                                deltaMaxChunkBuffer, &encodedSize,
                                CONTAINER_MAX_SIZE * 2, 0);
                            promotedDeltaSize = encodedSize;
                            if (oldBase.loadFromDisk)
                                free(oldBase.chunkPtr);

                            if (encodeResult == 0 && promotedDeltaSize > 0 &&
                                promotedDeltaSize < oldBaseStoredSize)
                            {
                                promotedDelta.assign(deltaMaxChunkBuffer,
                                                     deltaMaxChunkBuffer + promotedDeltaSize);
                                canPromote = true;
                            }
                        }
                    }

                    const int compressedSize = LZ4_compress_fast(
                        reinterpret_cast<const char *>(tmpChunk.chunkPtr),
                        reinterpret_cast<char *>(lz4ChunkBuffer),
                        tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
                    if (compressedSize > 0)
                    {
                        tmpChunk.deltaFlag = NO_DELTA;
                        tmpChunk.saveSize = compressedSize;
                    }
                    else
                    {
                        tmpChunk.deltaFlag = NO_LZ4;
                        tmpChunk.saveSize = tmpChunk.chunkSize;
                    }
                    tmpChunk.basechunkID = -1;
                    tmpChunkid = tmpChunk.chunkID;

                    // Write the new independent base first. A's promoted delta
                    // representation is appended separately below.
                    if (tmpChunk.deltaFlag == NO_LZ4)
                        dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);

                    bool replacedOldBase = false;
                    Chunk_t replacement;
                    if (canPromote)
                    {
                        replacement = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                        replacement.deltaFlag = DELTA;
                        replacement.basechunkID = tmpChunk.chunkID;
                        replacement.saveSize = promotedDeltaSize;
                        const auto appendStart = std::chrono::high_resolution_clock::now();
                        replacedOldBase = dataWrite_->Append_Replacement_Chunk(
                            replacement, promotedDelta.data());
                        rebaseAppendTime_ += std::chrono::high_resolution_clock::now() - appendStart;
                    }

                    if (replacedOldBase)
                    {
                        SuperFeatures oldBaseFeatures = table.feature_generator_.GenerateSuperFeatures(
                            string(reinterpret_cast<const char *>(oldBaseBytes.data()), oldBaseBytes.size()));
                        table.SF_Delete(oldBaseFeatures, basechunkid);
                        basechunkNum--;
                        basechunkSize -= oldBaseStoredSize;
                        LocalReduct -= oldBase.chunkSize - oldBaseStoredSize;
                        // DRR tracks only the active representation. The old
                        // bytes remain physically in the container, but are
                        // stale space and are excluded from this data-size metric.
                        uniquechunkSize -= oldBaseStoredSize;

                        replacement.chunkID = basechunkid;
                        replacement.chunkSize = oldBase.chunkSize;
                        replacement.saveSize = promotedDeltaSize;
                        StatsDelta(replacement);
                        uniquechunkSize += promotedDeltaSize;

                        rebaseDepth_[basechunkid] = rebaseDepth_[tmpChunk.chunkID] + 1;
                        const uint64_t dependentHeight = rebaseSubtreeHeight_[basechunkid];
                        rebaseSubtreeHeight_[tmpChunk.chunkID] = std::max(
                            rebaseSubtreeHeight_[tmpChunk.chunkID], dependentHeight + 1);
                        maxRebaseChainDepth_ = std::max(maxRebaseChainDepth_,
                            rebaseDepth_[basechunkid] + dependentHeight);
                        ++rebaseReplacementCount_;
                        rebaseBytesAppended_ += promotedDeltaSize;
                        rebaseStaleBytesRetained_ += oldBaseStoredSize;
                    }

                    if (tmpChunk.chunkSize > 60)
                        table.SF_Insert(superfeature, tmpChunk.chunkID);
                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    LocalReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
                    uniquechunkNum++;
                    uniquechunkSize += tmpChunk.saveSize;

                    if (tmpChunk.HeaderFlag == 0)
                        dataWrite_->Recipe_Insert(tmpChunk.chunkID);
                    else
                        dataWrite_->Recipe_Header_Insert(tmpChunk.chunkID);
                    logicalchunkNum++;
                    logicalchunkSize += tmpChunk.chunkSize;
                    continue;
                }

                // With an offline method, keep the online representation raw.
                // Tree-SF still builds rootChunkMap, but xdelta/LZ4 is deferred
                // completely to the offline Design3 pass.
                if (offlineMethod >= 0)
                {
                    tmpChunk.deltaFlag = NO_LZ4;
                    tmpChunk.basechunkID = -1;
                    tmpChunk.saveSize = tmpChunk.chunkSize;
                    tmpChunkid = tmpChunk.chunkID;
                    dataWrite_->Chunk_Insert(tmpChunk);

                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    uniquechunkNum++;
                    uniquechunkSize += tmpChunk.saveSize;

                    const uint64_t groupRootID = treeBaseChunkid == -1
                        ? tmpChunk.chunkID : treeBaseChunkid;
                    auto [groupIt, insertedGroup] = rootChunkMap->try_emplace(groupRootID);
                    if (IsLosslessGroupRoot(groupRootID))
                    {
                        if (insertedGroup)
                            ++GroupIndexEntryCount;
                        ++GroupIndexReferenceCount;
                    }
                    groupIt->second.push_back(tmpChunk.chunkID);

                    if (tmpChunk.HeaderFlag == 0)
                        dataWrite_->Recipe_Insert(tmpChunk.chunkID);
                    else
                        dataWrite_->Recipe_Header_Insert(tmpChunk.chunkID);
                    logicalchunkNum++;
                    logicalchunkSize += tmpChunk.chunkSize;
                    continue;
                }

                if (basechunkid != -1)
                // unique chunk & delta chunk
                {
                    auto basechunkInfo = dataWrite_->Get_Chunk_MetaInfo(basechunkid);
                    auto RestoreBasechunk = xd3_recursive_restore_BL_time(basechunkid);
                    uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);
                    if (RestoreBasechunk.loadFromDisk)
                        free(RestoreBasechunk.chunkPtr);
                    if (tmpChunk.saveSize == 0)
                    {
                        cout << "delta error" << endl;
                        return;
                    }
                    else if (tmpChunk.saveSize > tmpChunk.chunkSize) // error
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
                        tmpChunk.basechunkID = basechunkid;
                        // cout << "tmpChunk.savesize is " << tmpChunk.saveSize << endl;
                        memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                        StatsDelta(tmpChunk);
                        free(deltachunk);
                        // if (offlineMethod >= 0)
                        //     table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                        // if (basechunkInfo.loadFromDisk)
                        //     free(basechunkInfo.chunkPtr);
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
    if (ads_Version >= 100)
    {
        if (offlineMethod >= 0 && rootChunkMap != nullptr)
        {
            std::unordered_set<uint64_t> rootNodes;
            std::unordered_set<uint64_t> allNodes;

            // 遍历map，收集所有的根节点和子节点
            for (auto const &[rootId, children] : *rootChunkMap)
            {
                rootNodes.insert(rootId);
                for (uint64_t nodeId : children)
                {
                    allNodes.insert(nodeId);
                }
            }

            uint64_t leafNodeCount = 0;
            // 遍历所有出现过的节点
            for (uint64_t nodeId : allNodes)
            {
                // 如果一个节点不是根节点，那么它就是叶子节点
                if (rootNodes.find(nodeId) == rootNodes.end())
                {
                    leafNodeCount++;
                }
            }

            std::cout << "===== Leaf Node Count for Version " << ads_Version - 1 << " =====" << std::endl;
            std::cout << "Total unique nodes in map: " << allNodes.size() << std::endl;
            std::cout << "Root node count: " << rootNodes.size() << std::endl;
            std::cout << "Calculated leaf node count: " << leafNodeCount << std::endl;
            std::cout << "==========================================" << std::endl;
        }
    }
    recieveQueue->done_ = false;
    return;
}
