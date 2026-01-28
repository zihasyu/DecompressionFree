#include "../../../include/Thread/greedy.h"

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
struct RestoredChunk0
{
    size_t idx;
    Chunk_t tmpChunk;
    SuperFeatures superfeature;
};

Greedy::Greedy()
{

    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
    tmpDeltaBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    MinBaseBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    CombinedBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}

Greedy::~Greedy()
{
    free(lz4ChunkBuffer);
    free(deltaMaxChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(tmpDeltaBuffer);
    free(MinBaseBuffer);
    free(CombinedBuffer);
}

void Greedy::ProcessTrace()
{
    using namespace std;
    using namespace std::chrono;

    ThreadSafeQueue<RestoredChunk0> chunkQueue;
    vector<Chunk_t> &sourceList = dataWrite_->chunklist;
    size_t totalChunks = sourceList.size();

    // 恢复线程
    std::thread restoreThread([&]() {
        for (size_t i = 0; i < totalChunks; i++)
        {
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
            chunkQueue.push(RestoredChunk0{i, tmpChunk, superfeature});
        }
        chunkQueue.set_finished();
    });

    // 处理线程
    std::thread processThread([&]() {
        RestoredChunk0 item;
        size_t nextVersionEndPointIndex = 0;
        while (chunkQueue.pop(item))
        {
            size_t i = item.idx;
            Chunk_t &tmpChunk = item.tmpChunk;
            SuperFeatures &superfeature = item.superfeature;
            uint64_t basechunkid = -1;
            if (tmpChunk.chunkSize > 60)
            {
                basechunkid = table.SF_Find(superfeature);
            }

            if (basechunkid != -1)
            {
                auto RestoreBasechunk = FindBest(superfeature, tmpChunk);
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
                        table.SF_Insert(superfeature, tmpChunk.chunkID);
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
                        table.SF_Insert(superfeature, tmpChunk.chunkID);

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
                    table.SF_Insert(superfeature, tmpChunk.chunkID);
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
    });

    restoreThread.join();
    processThread.join();
    return;
}

Chunk_t Greedy::FindBest(SuperFeatures SF, const Chunk_t &Targetchunk)
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

uint8_t *Greedy::xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
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