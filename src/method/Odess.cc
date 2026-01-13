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

Odess::Odess(int offlineMethod)
    : offlineMethod(offlineMethod)
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

                if (offlineMethod >= 0)
                {
                    if (treeBaseChunkid == -1)
                    {
                        (*rootChunkMap)[tmpChunk.chunkID].push_back(tmpChunk.chunkID);
                    }
                    else
                    {
                        (*rootChunkMap)[treeBaseChunkid].push_back(tmpChunk.chunkID);
                    }
                }
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
            // [NEW] DCC (Delta Chaining Cohesion) a.k.a. "插队率" 统计
            double dcc_numerator = 0.0;
            double dcc_denominator = 0.0;

            for (const auto &pair : *rootChunkMap)
            {
                uint64_t key = pair.first;
                const std::vector<uint64_t> &nodes = pair.second;

                for (uint64_t node : nodes)
                {
                    if (key != node)
                    {
                        dcc_numerator++;
                    }
                    dcc_denominator++;
                }
            }

            double dcc_ratio = (dcc_denominator > 0) ? (dcc_numerator / dcc_denominator) : 0.0;

            std::cout << "===== DCC (插队率) for Version " << ads_Version - 1 << " =====" << std::endl;
            std::cout << "Total nodes compared (分母): " << dcc_denominator << std::endl;
            std::cout << "Mismatched nodes (分子): " << dcc_numerator << std::endl;
            std::cout << "DCC Ratio (分子/分母): " << dcc_ratio << std::endl;
            std::cout << "==========================================" << std::endl;
        }
    }
    recieveQueue->done_ = false;
    return;
}
