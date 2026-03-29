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

#endif
