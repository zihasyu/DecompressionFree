// topk_cache_policy.hpp
#pragma once

#include <unordered_map>
#include <set>
#include <limits>

namespace caches
{
template <typename Key>
class TopKCachePolicy
{
public:
    TopKCachePolicy() = default;

    void Insert(const Key& key)
    {
        freq_map[key] = 1;
        freq_set.insert({1, key});
    }

    void Touch(const Key& key)
    {
        auto it = freq_map.find(key);
        if (it != freq_map.end())
        {
            freq_set.erase({it->second, key});
            ++(it->second);
            freq_set.insert({it->second, key});
        }
    }

    void Erase(const Key& key)
    {
        auto it = freq_map.find(key);
        if (it != freq_map.end())
        {
            freq_set.erase({it->second, key});
            freq_map.erase(it);
        }
    }

    Key ReplCandidate() const
    {
        // 返回访问频率最低的 key
        if (!freq_set.empty())
        {
            return freq_set.begin()->second;
        }
        return Key{};
    }

private:
    // 记录每个 key 的访问次数
    std::unordered_map<Key, size_t> freq_map;
    // 按访问次数排序，方便查找最小值
    std::set<std::pair<size_t, Key>> freq_set;
};
} // namespace caches