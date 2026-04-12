#ifndef DESIGN_5_H
#define DESIGN_5_H

#include "../absmethod.h"
#include "../odess_similarity_detection.h"
#include "../chunkbufferpool.h"
#include "../lruCache.h"
#include <unordered_map>
#include <memory>
using namespace std;

class Design5 : public AbsMethod
{
private:
    struct HistoricalRewriteLogStats
    {
        uint64_t keptChunks = 0;
        uint64_t sourcedFromOffline = 0;
        uint64_t missingFromBoth = 0;
        uint64_t restoreFailures = 0;
        uint64_t rewrittenAsBase = 0;
        uint64_t rewrittenWithOriginalBase = 0;
        uint64_t rewrittenWithReplacementBase = 0;
        uint64_t rewrittenAsLz4Fallback = 0;
        uint64_t downgradedOldStoredSize = 0;
        uint64_t downgradedNewStoredSize = 0;
        uint64_t treeEdges = 0;
        uint64_t sfRetained = 0;
        uint64_t sfRemapped = 0;
        uint64_t sfRemoved = 0;
    };

    struct AppendLogStats
    {
        uint64_t inputRoots = 0;
        uint64_t inputChunks = 0;
        uint64_t appendedBaseChunks = 0;
        uint64_t appendedDeltaChunks = 0;
        uint64_t appendedLz4FallbackChunks = 0;
        uint64_t appendedSmallDeltaChunks = 0;
        uint64_t treeEdges = 0;
    };

    string myName_ = "Design5";
    int PrevDedupChunkid = -1;
    int Version = 0;
    uint8_t *MinBaseBuffer = nullptr;
    uint8_t *tmpDeltaBuffer = nullptr;
    uint64_t appendStart_ = 0;
    uint64_t appendEnd_ = 0;
    size_t generationId_ = 0;

    size_t cacheHitCount = 0;
    size_t cacheAccessCount = 0;
    std::unordered_map<uint64_t, int> chunkHotMap;
    std::unordered_map<uint64_t, uint64_t> logicalRootMap;
    std::unordered_map<uint64_t, uint64_t> lastChildMap;
    std::unordered_map<uint64_t, SuperFeatures> searchableChunkSFs_;

    uint8_t *bro_basechunk_ptr_cache = nullptr;
    uint8_t *chi_basechunk_ptr_cache = nullptr;
    uint8_t *basechunk_ptr_cache = nullptr;
    ChunkBufferPool<MAX_CHUNK_SIZE, 1024> chunkCache_;
    HistoricalRewriteLogStats historicalLogStats_;
    AppendLogStats appendLogStats_;
    uint64_t searchableChunkCount_ = 0;
    uint64_t historicalOnlyStoredSize_ = 0;

    Chunk_t LoadSourceChunk(uint64_t chunkId);
    Chunk_t RestoreChunkFromWriter(dataWrite *writer, uint64_t chunkId);
    void ResetSearchState();
    void AppendChild(uint64_t parentId, uint64_t childId);
    std::string PrepareNextGenerationPath();
    bool ChunkExists(const dataWrite *writer, uint64_t chunkId) const;
    bool IsSearchableChunk(const Chunk_t &chunk) const;
    bool OwnsSuperFeature(uint64_t chunkId, super_feature_t sf) const;
    void RecordSearchableChunkSF(uint64_t chunkId, const Chunk_t &rawChunk);
    int ResolveChildAnchor(dataWrite *writer, int baseChunkId) const;
    int ResolveReplacementBase(dataWrite *writer, int baseChunkId, uint64_t currentChunkId) const;
    int FindReplacementEntryInSubtree(dataWrite *writer, uint64_t rootId, super_feature_t sf) const;
    bool RewriteChunkAsLz4Base(const Chunk_t &sourceMeta, Chunk_t &rawChunk);
    bool RewriteChunkWithOriginalDelta(dataWrite *sourceWriter, const Chunk_t &sourceMeta, const Chunk_t &rawChunk);
    bool RewriteChunkWithReplacementBase(const Chunk_t &sourceMeta, Chunk_t &rawChunk, int replacementBaseId);
    void RewriteKeptHistoricalChunks(dataWrite *sourceWriter,
                                     const std::unordered_map<super_feature_t, uint64_t> &oldTreeIndex);
    void RepairTreeIndexFromHistoricalState(dataWrite *sourceWriter,
                                            const std::unordered_map<super_feature_t, uint64_t> &oldTreeIndex);
    void RegisterNewSearchableChunk(uint64_t chunkId, const Chunk_t &rawChunk);
    void ResetOfflineStatsForRebuild();
    void ResetRebuildLogStats();
    void FinalizeRebuildLogStats();
    void PrintRebuildLogStats() const;

public:
    Design5();
    ~Design5();
    void SetAppendRange(uint64_t appendStart, uint64_t appendEnd);
    uint64_t GetHistoricalOnlyStoredSize() const { return historicalOnlyStoredSize_; }
    void ProcessTrace();
    uint8_t *xd3_encode_buffer(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer);
    Chunk_t xd3_recursive_restore_BL_time(uint64_t BasechunkId);
    Chunk_t CutGreedy(uint64_t BasechunkId, const Chunk_t Targetchunk);
    void StatsHit(uint64_t FatherID, uint64_t HitID, uint64_t BasechunkID);
};
#endif
