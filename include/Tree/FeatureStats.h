#ifndef FEATURE_CACHE_H
#define FEATURE_CACHE_H

#include <vector>
#include <deque>
#include <numeric>
#include <cmath>
#include <cstdint>

// 假设 current_chunk_count 是一个全局可访问的变量
extern uint64_t logicalchunkNum;

struct FeatureStats
{
    uint64_t feature_hash; // 使用SF的哈希作为ID
    uint64_t access_count = 0;
    uint64_t cache_hits = 0;

    // 访问模式记录
    std::deque<uint64_t> access_intervals;
    uint64_t last_access_chunk_id = 0;
    uint64_t first_access_chunk_id = 0;

    // Feature树特征 (首次加载时记录)
    int tree_depth = 0;
    double avg_children_count = 0.0;

    FeatureStats(uint64_t hash) : feature_hash(hash) {}

    void record_access()
    {
        access_count++;
        if (first_access_chunk_id == 0)
        {
            first_access_chunk_id = logicalchunkNum;
        }
        if (last_access_chunk_id != 0)
        {
            access_intervals.push_back(logicalchunkNum - last_access_chunk_id);
            if (access_intervals.size() > 20)
            {
                access_intervals.pop_front();
            }
        }
        last_access_chunk_id = logicalchunkNum;
    }

    double predict_future_activity()
    {
        if (access_intervals.size() < 3)
        {
            return 0.5; // 数据不足，中性评分
        }

        // 1. 周期性检测
        double sum = std::accumulate(access_intervals.begin(), access_intervals.end(), 0.0);
        double mean = sum / access_intervals.size();
        double sq_sum = std::inner_product(access_intervals.begin(), access_intervals.end(), access_intervals.begin(), 0.0);
        double std_dev = std::sqrt(sq_sum / access_intervals.size() - mean * mean);
        double periodicity = 1.0 - std::min(1.0, std_dev / (mean + 1.0));

        // 2. 最近活跃度
        uint64_t chunks_since_last = logicalchunkNum - last_access_chunk_id;
        double recency = 1.0 / (1.0 + static_cast<double>(chunks_since_last) / 10000.0);

        return periodicity * 0.7 + recency * 0.3;
    }

    double calculate_importance()
    {
        if (access_count == 0)
            return 0.0;

        // 维度1: 访问频率
        uint64_t age = (logicalchunkNum > first_access_chunk_id) ? (logicalchunkNum - first_access_chunk_id) : 1;
        double frequency_score = static_cast<double>(access_count) / age;

        // 维度2: 解压成本
        double decompression_cost = tree_depth * 2.0 + avg_children_count * 1.5;

        // 维度3: 命中率
        double hit_rate = static_cast<double>(cache_hits) / access_count;

        // 维度4: 活跃度
        double activity_score = predict_future_activity();

        // 综合评分
        return (frequency_score * 0.3 +
                decompression_cost * 0.3 +
                hit_rate * 0.2 +
                activity_score * 0.2);
    }
};

#endif // FEATURE_CACHE_H