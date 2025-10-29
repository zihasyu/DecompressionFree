#ifndef FEATURE_STATS_H
#define FEATURE_STATS_H

#include <cstdint>

/**
 * @brief 简化版的特征统计信息，只记录访问频率。
 */
struct FeatureStats
{
    uint64_t feature_hash;     // 特征的唯一标识 (来自SuperFeature的哈希)
    uint64_t access_count = 0; // 该特征被访问的总次数

    FeatureStats(uint64_t hash) : feature_hash(hash) {}

    /**
     * @brief 记录一次访问，增加访问计数。
     */
    void record_access()
    {
        access_count++;
    }

    /**
     * @brief 计算重要性。在此简化模型中，重要性直接等同于访问次数。
     * @return double 访问次数。
     */
    double calculate_importance() const
    {
        return static_cast<double>(access_count);
    }
};

#endif // FEATURE_STATS_H