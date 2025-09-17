#ifndef TREE_PRE_H
#define TREE_PRE_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../lruCache.h"
#include <unordered_map>

using namespace std;

class TreePre : public AbsMethod
{
private:
    string myName_ = "TreePre";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;

    lru11::Cache<uint64_t, std::vector<uint8_t>, std::mutex> chunkCache;
    lru11::Cache<uint64_t, std::vector<uint8_t>, std::mutex> chunkCache2;
    uint8_t *CombinedBuffer_thread;
    uint8_t *DecodeBuffer_thread;
    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    size_t cache2HitCount = 0;
    size_t cache2AccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;

    std::unordered_map<uint64_t, uint64_t> Prev_Chunk_seq_map;
    std::unordered_map<uint64_t, uint64_t> Chunk_seq_map;

    std::thread prefetch_thread;
    std::atomic<bool> stop_prefetch{false};
    std::queue<uint64_t> prefetch_queue;
    std::mutex prefetch_mutex;
    std::condition_variable prefetch_cv;

    void PrefetchThreadFunc();
    void RequestPrefetch(uint64_t chunk_id);

public:
    TreePre();
    ~TreePre();
    void ProcessTrace();
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk, SuperFeatures sfs);
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    void StatsFit(uint64_t FatherID, uint64_t FitID, SuperFeatures sfs);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    Chunk_t xd3_recursive_restore_BL_thread(uint64_t BasechunkId);
    uint8_t *xd3_decode_thread(const uint8_t *in, size_t in_size, const uint8_t *ref, size_t ref_size, size_t *res_size);
};
#endif