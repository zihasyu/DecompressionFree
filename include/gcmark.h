#ifndef GC_MARK_H
#define GC_MARK_H

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "datawrite.h"

struct GCMarkState
{
    std::vector<uint32_t> dedupRefCount;
    std::vector<uint8_t> keepChunk;
    std::vector<std::string> keptBackups;
    std::vector<std::string> expiredBackups;
    std::unordered_set<std::string> keptBackupSet;
    size_t preservedBaseChunks = 0;
    uint64_t preservedBaseStoredSize = 0;
    uint64_t preservedProtectedBytes = 0;

    bool ShouldKeepChunk(uint64_t chunkId) const
    {
        return chunkId < keepChunk.size() && keepChunk[chunkId] != 0;
    }
};

inline size_t ResolveRetentionWindow(int retentionBackups, size_t processedBackupCount)
{
    if (retentionBackups <= 0)
    {
        return processedBackupCount;
    }
    return std::min<size_t>(retentionBackups, processedBackupCount);
}

inline GCMarkState BuildGCMarkState(const dataWrite &dataWriteObj,
                                    const std::vector<std::string> &processedBackups,
                                    int retentionBackups)
{
    GCMarkState state;
    state.dedupRefCount.assign(dataWriteObj.chunklist.size(), 0);
    state.keepChunk.assign(dataWriteObj.chunklist.size(), 0);

    const size_t window = ResolveRetentionWindow(retentionBackups, processedBackups.size());
    const size_t keepBegin = processedBackups.size() > window ? processedBackups.size() - window : 0;

    for (size_t i = 0; i < processedBackups.size(); ++i)
    {
        const std::string &backup = processedBackups[i];
        if (i >= keepBegin)
        {
            state.keptBackups.push_back(backup);
            state.keptBackupSet.insert(backup);
        }
        else
        {
            state.expiredBackups.push_back(backup);
        }
    }

    for (const auto &backup : state.keptBackups)
    {
        auto recipeIt = dataWriteObj.RecipeMap.find(backup);
        if (recipeIt == dataWriteObj.RecipeMap.end())
        {
            continue;
        }

        for (uint64_t chunkId : recipeIt->second)
        {
            if (chunkId >= state.dedupRefCount.size())
            {
                continue;
            }
            state.dedupRefCount[chunkId]++;
        }
    }

    for (size_t i = 0; i < state.dedupRefCount.size(); ++i)
    {
        if (state.dedupRefCount[i] > 0)
        {
            state.keepChunk[i] = 1;
        }
    }

    return state;
}

inline void PreserveReferencedBaseChunks(GCMarkState &state,
                                         const dataWrite &offlineDataWrite,
                                         uint32_t minKeptChildren = 4)
{
    const size_t limit = std::min(state.keepChunk.size(), offlineDataWrite.chunklist.size());
    if (limit == 0)
    {
        return;
    }

    std::vector<uint32_t> keptChildCount(limit, 0);
    std::vector<uint64_t> protectedBytes(limit, 0);

    for (size_t chunkId = 0; chunkId < limit; ++chunkId)
    {
        if (!state.ShouldKeepChunk(chunkId))
        {
            continue;
        }

        const Chunk_t &chunk = offlineDataWrite.chunklist[chunkId];
        if (chunk.chunkSize == 0 || chunk.deltaFlag != DELTA || chunk.basechunkID < 0)
        {
            continue;
        }

        const size_t baseId = static_cast<size_t>(chunk.basechunkID);
        if (baseId >= limit)
        {
            continue;
        }

        keptChildCount[baseId]++;
        if (chunk.chunkSize > chunk.saveSize)
        {
            protectedBytes[baseId] += chunk.chunkSize - chunk.saveSize;
        }
    }

    struct Candidate
    {
        size_t chunkId;
        uint32_t keptChildren;
        uint64_t protectedBytes;
        uint64_t storedSize;
        bool positivePayoff;
    };

    std::vector<Candidate> candidates;
    for (size_t chunkId = 0; chunkId < limit; ++chunkId)
    {
        if (state.keepChunk[chunkId] != 0)
        {
            continue;
        }

        const Chunk_t &chunk = offlineDataWrite.chunklist[chunkId];
        if (chunk.chunkSize == 0 || keptChildCount[chunkId] == 0)
        {
            continue;
        }

        const uint64_t storedSize = chunk.saveSize;
        const bool positivePayoff = protectedBytes[chunkId] > storedSize;
        if (keptChildCount[chunkId] < minKeptChildren && !positivePayoff)
        {
            continue;
        }

        candidates.push_back(Candidate{
            chunkId,
            keptChildCount[chunkId],
            protectedBytes[chunkId],
            storedSize,
            positivePayoff});
    }

    std::sort(candidates.begin(), candidates.end(), [](const Candidate &lhs, const Candidate &rhs)
              {
        if (lhs.positivePayoff != rhs.positivePayoff)
        {
            return lhs.positivePayoff > rhs.positivePayoff;
        }
        if (lhs.keptChildren != rhs.keptChildren)
        {
            return lhs.keptChildren > rhs.keptChildren;
        }
        if (lhs.protectedBytes != rhs.protectedBytes)
        {
            return lhs.protectedBytes > rhs.protectedBytes;
        }
        return lhs.storedSize < rhs.storedSize; });

    for (const Candidate &candidate : candidates)
    {
        state.keepChunk[candidate.chunkId] = 1;
        state.preservedBaseChunks++;
        state.preservedBaseStoredSize += candidate.storedSize;
        state.preservedProtectedBytes += candidate.protectedBytes;
    }
}

#endif
