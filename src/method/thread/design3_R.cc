#include "../../../include/Thread/design3_R.h"

// const size_t TREE_INSERT_SAVE_THRESHOLD = 128;

Design3_R::Design3_R()
{
    // cout << " Chunk_t is " << sizeof(Chunk_t) << " Chunk_t_ori is " << sizeof(Chunk_t_odess) << " <super_feature_t, unordered_set<string>> is " << sizeof(super_feature_t);
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    // [NEW] Allocate buffer for CutGreedy if needed
    CombinedBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

Design3_R::~Design3_R()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
    // [NEW] Free the allocated buffer
    free(CombinedBuffer);
}

// 线程安全队列
template <typename T>
class ThreadSafeQueue
{
public:
    void push(const T &value)
    {
        std::lock_guard<std::mutex> lk(m_);
        q_.push(value);
        cv_.notify_one();
    }
    bool pop(T &value)
    {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this] { return !q_.empty() || finished_; });
        if (q_.empty())
            return false;
        value = std::move(q_.front());
        q_.pop();
        return true;
    }
    void set_finished()
    {
        std::lock_guard<std::mutex> lk(m_);
        finished_ = true;
        cv_.notify_all();
    }
private:
    std::queue<T> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool finished_ = false;
};

// 用于线程间传递的结构体
struct RestoredChunk3R
{
    size_t idx;
    Chunk_t tmpChunk;
    SuperFeatures superfeature;
};

void Design3_R::ProcessTrace()
{
    using namespace std;
    using namespace std::chrono;

    ThreadSafeQueue<RestoredChunk3R> chunkQueue;
    vector<Chunk_t> &sourceList = dataWrite_->chunklist;
    size_t totalChunks = sourceList.size();

    // 恢复线程
    std::thread restoreThread([&]() {
        for (size_t i = 0; i < totalChunks; i++)
        {
            auto startRestoreChunk = high_resolution_clock::now();
            Chunk_t tmpChunk = dataWrite_->Get_Chunk_MetaInfo(i);
            if (tmpChunk.basechunkID >= 0)
            {
                Chunk_t tmpPreChunk = dataWrite_->Get_Chunk_Info(tmpChunk.basechunkID);
                Chunk_t tmpDeltaChunk = dataWrite_->Get_Chunk_Info(i);
                uint64_t tmpSize = 0;
                tmpChunk.chunkPtr = xd3_decode(tmpDeltaChunk.chunkPtr, tmpDeltaChunk.saveSize, tmpPreChunk.chunkPtr, tmpPreChunk.chunkSize, &tmpSize);
                tmpChunk.loadFromDisk = true;
                if (tmpPreChunk.loadFromDisk)
                    free(tmpPreChunk.chunkPtr);
                if (tmpDeltaChunk.loadFromDisk)
                    free(tmpDeltaChunk.chunkPtr);
            }
            else
            {
                Chunk_t rawChunk = dataWrite_->Get_Chunk_Info(i);
                tmpChunk = rawChunk;
                tmpChunk.chunkPtr = (uint8_t *)malloc(tmpChunk.chunkSize);
                if (tmpChunk.chunkPtr != nullptr && rawChunk.chunkPtr != nullptr)
                {
                    memcpy(tmpChunk.chunkPtr, rawChunk.chunkPtr, tmpChunk.chunkSize);
                }
                tmpChunk.loadFromDisk = true;
                if (rawChunk.loadFromDisk)
                {
                    free(rawChunk.chunkPtr);
                }
            }
            auto endRestoreChunk = high_resolution_clock::now();
            RestoreChunkTime += endRestoreChunk - startRestoreChunk;

            // 计算superfeature
            string tmpChunkContent((char *)tmpChunk.chunkPtr, tmpChunk.chunkSize);
            SuperFeatures superfeature;
            if (tmpChunk.chunkSize > 60)
            {
                startSF = high_resolution_clock::now();
                superfeature = table.feature_generator_.GenerateSuperFeatures(tmpChunkContent);
                endSF = high_resolution_clock::now();
                SFTime += (endSF - startSF);
            }
            chunkQueue.push(RestoredChunk3R{i, tmpChunk, superfeature});
        }
        chunkQueue.set_finished();
    });

    // 处理线程
    std::thread processThread([&]() {
        RestoredChunk3R item;
        size_t nextVersionEndPointIndex = 0;
        while (chunkQueue.pop(item))
        {
            size_t i = item.idx;
            Chunk_t &tmpChunk = item.tmpChunk;
            SuperFeatures &superfeature = item.superfeature;
            uint64_t basechunkid = -1;
            if (tmpChunk.chunkSize > 60)
            {
                basechunkid = table.Tree_SF_Find(superfeature);
            }

            if (basechunkid != -1)
            {
                auto RestoreBasechunk = CutGreedy(basechunkid, tmpChunk, superfeature);
                uint8_t *deltachunk = xd3_encode(tmpChunk.chunkPtr, tmpChunk.chunkSize, RestoreBasechunk.chunkPtr, RestoreBasechunk.chunkSize, &tmpChunk.saveSize, deltaMaxChunkBuffer);

                if (RestoreBasechunk.loadFromDisk)
                    free(RestoreBasechunk.chunkPtr);

                if (tmpChunk.saveSize > tmpChunk.chunkSize || tmpChunk.saveSize <= 0 || RestoreBasechunk.chunkSize == 0)
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
                        table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                    basechunkNum++;
                    basechunkSize += tmpChunk.saveSize;
                    free(deltachunk);

                    if (tmpChunk.deltaFlag == NO_LZ4)
                        offline_dataWrite_->Chunk_Insert(tmpChunk);
                    else
                        offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
                }
                else
                {
                    tmpChunk.deltaFlag = DELTA;
                    tmpChunk.basechunkID = RestoreBasechunk.chunkID;

                    if (tmpChunk.chunkSize > 60)
                        table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);

                    if(tmpChunk.saveSize >= TREE_INSERT_SAVE_THRESHOLD){
                        if (offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID < 0)
                        {
                            offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID = tmpChunk.chunkID;
                        }
                        else
                        {
                            int broID = offline_dataWrite_->chunklist[tmpChunk.basechunkID].FirstChildID;
                            while (offline_dataWrite_->chunklist[broID].FirstBroID >= 0)
                                broID = offline_dataWrite_->chunklist[broID].FirstBroID;
                            offline_dataWrite_->chunklist[broID].FirstBroID = tmpChunk.chunkID;
                        }
                    }

                    memcpy(tmpChunk.chunkPtr, deltachunk, tmpChunk.saveSize);
                    StatsDelta(tmpChunk);
                    free(deltachunk);

                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                }
            }
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
                    table.Tree_SF_Insert(superfeature, tmpChunk.chunkID);
                basechunkNum++;
                basechunkSize += tmpChunk.saveSize;

                if (tmpChunk.deltaFlag == NO_LZ4)
                    offline_dataWrite_->Chunk_Insert(tmpChunk);
                else
                    offline_dataWrite_->Chunk_Insert(tmpChunk, lz4ChunkBuffer);
            }

            uniquechunkNum++;
            uniquechunkSize += tmpChunk.saveSize;
            logicalchunkNum++;
            logicalchunkSize += tmpChunk.chunkSize;
            if ((i + 1) == dataWrite_->versionEndPoints[nextVersionEndPointIndex])
            {
                nextVersionEndPointIndex++;
                cout << "----------------------offline compression-------------------------" << std::endl;
                cout << "version " << nextVersionEndPointIndex << " processed" << std::endl;
                cout << " process chunks: " << (i + 1) << std::endl;
                cout << "  unique chunk count: " << uniquechunkNum << ", size: " << uniquechunkSize << std::endl;
                cout << "  base chunk count: " << basechunkNum << ", size: " << basechunkSize << std::endl;
                cout << "  logical chunk count: " << logicalchunkNum << ", size: " << logicalchunkSize << std::endl;
                cout << "  unique ratio: " << (double)uniquechunkSize / logicalchunkSize << std::endl;
                cout << "  base ratio: " << (double)basechunkSize / logicalchunkSize << std::endl;
                cout << "  SFTime: " << SFTime.count() << "s" << std::endl;
                cout << "  MiDeltaTime: " << MiDeltaTime.count() << "s" << std::endl;
                cout << "  EncodeTime: " << EncodeTime.count() << "s" << std::endl;
            }
        }
        // Finalize
        ads_Version++;
        SFnum = basechunkNum * 3;
    });

    restoreThread.join();
    processThread.join();
    return;
}
Chunk_t Design3_R::CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs)
{
    SetTime(startMiDelta);
    Chunk_t resultchunk;
    size_t basechunk_size = 0;

    Chunk_t basechunk = offline_dataWrite_->Get_Chunk_MetaInfo(BasechunkId);
    if (basechunk.basechunkID < 0)
    {
        SetTime(startIO);
        basechunk = offline_dataWrite_->Get_Chunk_Info(BasechunkId);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);
        if (basechunk.FirstChildID < 0) // if only one layer
            return basechunk;
        // basechunk = xd3_recursive_restore_BL_time(BasechunkId);
    }
    else
    {
        basechunk = xd3_recursive_restore_offline_time(BasechunkId);
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
        Chunk_t TmpChildChunk = offline_dataWrite_->Get_Chunk_Info(resultchunk.FirstChildID);
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
            TmpBroChunk = offline_dataWrite_->Get_Chunk_Info(TmpBroChunk.FirstBroID);
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
        StatsHit(tmpFatherID, resultchunk.chunkID, sfs);
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

uint8_t *Design3_R::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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

void Design3_R::StatsHit(uint64_t FatherID, uint64_t HitID, SuperFeatures sfs)
{
    // [CHANGE] StatsHit should operate on the destination data (offline_dataWrite_)
    if (offline_dataWrite_->chunklist[FatherID].BeforeFit == HitID)
    {
        offline_dataWrite_->chunklist[FatherID].HitCount++;
    }
    else
    {
        offline_dataWrite_->chunklist[FatherID].BeforeFit = HitID;
        offline_dataWrite_->chunklist[FatherID].HitCount = 1;
    }
    if (offline_dataWrite_->chunklist[FatherID].HitCount > 4)
    {
        if (table.Tree_SF_Find(sfs) == FatherID)
            table.Tree_SF_ReWrite(sfs, HitID);
    }
}
