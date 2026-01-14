/**
 * \file
 * \brief Cache policy interface declaration
 */
#ifndef CACHE_POLICY_HPP
#define CACHE_POLICY_HPP

#include <unordered_set>

namespace caches
{

/**
 * \brief Cache policy abstract base class
 * \tparam Key Type of a key a policy works with
 */
template <typename Key>
class ICachePolicy
{
  public:
    virtual ~ICachePolicy() = default;

    /**
     * \brief Handle element insertion in a cache
     * \param[in] key Key that should be used by the policy
     */
    virtual void Insert(const Key &key) = 0;

    /**
     * \brief Handle request to the key-element in a cache
     * \param key
     */
    virtual void Touch(const Key &key) = 0;
    /**
     * \brief Handle element deletion from a cache
     * \param[in] key Key that should be used by the policy
     */
    virtual void Erase(const Key &key) = 0;

    /**
     * \brief Return a key of a replacement candidate
     * \return Replacement candidate according to selected policy
     */
    virtual const Key &ReplCandidate() const = 0;
};

/**
 * \brief Basic no caching policy class
 * \details Preserve any key provided. Erase procedure can get rid of any added keys
 * without specific rules: a replacement candidate will be the first element in the
 * underlying container. As unordered container can be used in the implementation
 * there are no warranties that the first/last added key will be erased
 * \tparam Key Type of a key a policy works with
 */
template <typename Key>
class NoCachePolicy : public ICachePolicy<Key>
{
  public:
    NoCachePolicy() = default;
    ~NoCachePolicy() noexcept override = default;

    void Insert(const Key &key) override
    {
        key_storage.emplace(key);
    }

    void Touch(const Key &key) noexcept override
    {
        // do not do anything
        (void)key;
    }

    void Erase(const Key &key) noexcept override
    {
        key_storage.erase(key);
    }

    // return a key of a displacement candidate
    const Key &ReplCandidate() const noexcept override
    {
        return *key_storage.cbegin();
    }

  private:
    std::unordered_set<Key> key_storage;
};

template <typename Key>
class LFUCachePolicy : public ICachePolicy<Key>
{
public:
    void Insert(const Key &key) override {
        freq[key] = 1;
        keys_by_freq[1].insert(key);
    }
    void Touch(const Key &key) override {
        int f = freq[key];
        keys_by_freq[f].erase(key);
        if (keys_by_freq[f].empty()) keys_by_freq.erase(f);
        freq[key] = f + 1;
        keys_by_freq[f + 1].insert(key);
    }
    void Erase(const Key &key) override {
        int f = freq[key];
        keys_by_freq[f].erase(key);
        if (keys_by_freq[f].empty()) keys_by_freq.erase(f);
        freq.erase(key);
    }
    const Key &ReplCandidate() const override {
        auto it = keys_by_freq.begin();
        return *(it->second.begin());
    }
private:
    std::unordered_map<Key, int> freq;
    std::map<int, std::set<Key>> keys_by_freq;
};

template <typename Key>
class LRUCachePolicy : public ICachePolicy<Key>
{
public:
    void Insert(const Key &key) override {
        lru_list.push_front(key);
        lru_map[key] = lru_list.begin();
    }
    void Touch(const Key &key) override {
        auto it = lru_map.find(key);
        if (it != lru_map.end()) {
            lru_list.splice(lru_list.begin(), lru_list, it->second);
        }
    }
    void Erase(const Key &key) override {
        auto it = lru_map.find(key);
        if (it != lru_map.end()) {
            lru_list.erase(it->second);
            lru_map.erase(it);
        }
    }
    const Key &ReplCandidate() const override {
        return lru_list.back();
    }
private:
    std::list<Key> lru_list;
    std::unordered_map<Key, typename std::list<Key>::iterator> lru_map;
};

/**
 * \brief ARC (Adaptive Replacement Cache) policy class
 * \tparam Key Type of a key a policy works with
 */
template <typename Key>
class ARCCachePolicy : public ICachePolicy<Key>
{
public:
    ARCCachePolicy(size_t capacity = 100)
        : c(capacity), p(0) {}

    void Insert(const Key &key) override
    {
        // Case 1: Key is in B1 (ghost list for T1)
        if (b1_map.count(key)) {
            size_t delta = 1;
            if (b2_map.size() > 0 && b1_map.size() < b2_map.size()) {
                delta = b2_map.size() / b1_map.size();
            }
            p = std::min(c, p + delta);
            Replace(key);
            b1_list.erase(b1_map[key]);
            b1_map.erase(key);
            t2_list.push_front(key);
            t2_map[key] = t2_list.begin();
        }
        // Case 2: Key is in B2 (ghost list for T2)
        else if (b2_map.count(key)) {
            size_t delta = 1;
            if (b1_map.size() > 0 && b2_map.size() < b1_map.size()) {
                delta = b1_map.size() / b2_map.size();
            }
            p = (p >= delta) ? p - delta : 0;
            Replace(key);
            b2_list.erase(b2_map[key]);
            b2_map.erase(key);
            t2_list.push_front(key);
            t2_map[key] = t2_list.begin();
        }
        // Case 3: Key is not in cache or ghost lists
        else {
            if (t1_map.size() + b1_map.size() == c) {
                if (t1_map.size() < c) {
                    auto last = b1_list.back();
                    b1_list.pop_back();
                    b1_map.erase(last);
                    Replace(key);
                } else {
                    auto last = t1_list.back();
                    t1_list.pop_back();
                    t1_map.erase(last);
                }
            } else if (t1_map.size() + b1_map.size() + t2_map.size() + b2_map.size() >= c) {
                if (t1_map.size() + b1_map.size() + t2_map.size() + b2_map.size() == 2 * c) {
                    auto last = b2_list.back();
                    b2_list.pop_back();
                    b2_map.erase(last);
                }
                Replace(key);
            }
            t1_list.push_front(key);
            t1_map[key] = t1_list.begin();
        }
    }

    void Touch(const Key &key) override
    {
        // If key is in T1, move to T2
        if (t1_map.count(key)) {
            t1_list.erase(t1_map[key]);
            t1_map.erase(key);
            t2_list.push_front(key);
            t2_map[key] = t2_list.begin();
        }
        // If key is in T2, move to front
        else if (t2_map.count(key)) {
            t2_list.erase(t2_map[key]);
            t2_list.push_front(key);
            t2_map[key] = t2_list.begin();
        }
    }

    void Erase(const Key &key) override
    {
        if (t1_map.count(key)) {
            t1_list.erase(t1_map[key]);
            t1_map.erase(key);
            b1_list.push_front(key);
            b1_map[key] = b1_list.begin();
        } else if (t2_map.count(key)) {
            t2_list.erase(t2_map[key]);
            t2_map.erase(key);
            b2_list.push_front(key);
            b2_map[key] = b2_list.begin();
        }
        // If key is in ghost lists, remove it completely
        else if (b1_map.count(key)) {
            b1_list.erase(b1_map[key]);
            b1_map.erase(key);
        } else if (b2_map.count(key)) {
            b2_list.erase(b2_map[key]);
            b2_map.erase(key);
        }
    }

    const Key &ReplCandidate() const override
    {
        // Check if cache has any elements
        if (t1_list.empty() && t2_list.empty()) {
            throw std::runtime_error("Cannot get replacement candidate from empty cache");
        }
        
        // Replacement candidate is the last element in T1 or T2, depending on p
        if (!t1_list.empty() && (t1_list.size() > p || t2_list.empty())) {
            return t1_list.back();
        } else if (!t2_list.empty()) {
            return t2_list.back();
        }
        
        // This should never be reached
        throw std::runtime_error("Unexpected state in ReplCandidate");
    }

    // Helper method to check if cache contains a key
    bool Contains(const Key &key) const
    {
        return t1_map.count(key) > 0 || t2_map.count(key) > 0;
    }

    // Helper method to get current cache size
    size_t Size() const
    {
        return t1_map.size() + t2_map.size();
    }

    // Helper method to check if cache is empty
    bool Empty() const
    {
        return t1_map.empty() && t2_map.empty();
    }

private:
    void Replace(const Key &key)
    {
        if (!t1_list.empty() && (t1_list.size() > p || (b2_map.count(key) && t1_list.size() == p))) {
            auto last = t1_list.back();
            t1_list.pop_back();
            t1_map.erase(last);
            b1_list.push_front(last);
            b1_map[last] = b1_list.begin();
        } else if (!t2_list.empty()) {
            auto last = t2_list.back();
            t2_list.pop_back();
            t2_map.erase(last);
            b2_list.push_front(last);
            b2_map[last] = b2_list.begin();
        }
    }

    size_t c; // cache capacity
    size_t p; // target size for T1

    // Main lists and maps
    std::list<Key> t1_list, t2_list, b1_list, b2_list;
    std::unordered_map<Key, typename std::list<Key>::iterator> t1_map, t2_map, b1_map, b2_map;
};

} // namespace caches

#endif // CACHE_POLICY_HPP
