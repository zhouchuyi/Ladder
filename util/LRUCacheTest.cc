#include "./LRUCache.h"
#include <gtest/gtest.h>
#include <vector>
#include <iostream>

static void* EncodeValue(uintptr_t value) { return reinterpret_cast<void*>(value); }
static int DecodeValue(void* value) { return reinterpret_cast<uintptr_t>(value); }
static std::string EncodeKey(int k) {
    return std::to_string(k);
}
static int DecodeKey(const std::string& k) {
  return std::stoi(k);
}
class CacheTest : public testing::Test
{
public:
    static CacheTest* current_;
    static constexpr int kCacheSize = 1000;
    std::vector<int> deletedKeys_;
    std::vector<int> deletedValues_;
    std::shared_ptr<ShardedLRUCache> cache_;
    static void Deleter(const std::string& key,void* v)
    {
        // assert(current_ != nullptr);
        current_->deletedKeys_.push_back(DecodeKey(key));
        current_->deletedValues_.push_back(DecodeValue(v));
    }

public:
    CacheTest()
     : cache_(ShardedLRUCache::NewCache(kCacheSize))
    {
        current_ = this;
    }
    ~CacheTest()
    {
        // current_ = nullptr;
    }
    int Lookup(int key)
    {
        Entry* e = cache_->Lookup(EncodeKey(key));
        int r = (e == nullptr) ? -1 : DecodeValue((e->value_));
        if(r != -1)
        {
            cache_->Release(e);
        }
        return r;
    }

    void Insert(int key,int v,int charge = 1)
    {
        Entry* e = cache_->Insert(EncodeKey(key),EncodeValue(v),1,Deleter);
        cache_->Release(e);
    }

    Entry* InsertAndReturnEntry(int key,int v,int charge = 1)
    {
        Entry* e = cache_->Insert(EncodeKey(key),EncodeValue(v),1,Deleter);
        return e;
    }

    void Erase(int key)
    {
        cache_->Erase(EncodeKey(key));
    }

    std::string pairToString(const std::pair<size_t,size_t>& value)
    { 
		return std::to_string(value.first) + std::to_string(value.second);
    }
};

CacheTest* CacheTest::current_ = nullptr;

TEST_F(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  ASSERT_EQ(1, deletedKeys_.size());
  ASSERT_EQ(100, deletedKeys_[0]);
  ASSERT_EQ(101, deletedValues_[0]);
}

TEST_F(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0, deletedKeys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deletedKeys_.size());
  ASSERT_EQ(100, deletedKeys_[0]);
  ASSERT_EQ(101, deletedValues_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deletedKeys_.size());
}

TEST_F(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Entry* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(h1->value_));

  Insert(100, 102);
  Entry* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(h2->value_));
  ASSERT_EQ(0, deletedKeys_.size());

  cache_->Release(h1);
  ASSERT_EQ(1, deletedKeys_.size());
  ASSERT_EQ(100, deletedKeys_[0]);
  ASSERT_EQ(101, deletedValues_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, deletedKeys_.size());

  cache_->Release(h2);
  ASSERT_EQ(2, deletedKeys_.size());
  ASSERT_EQ(100, deletedKeys_[1]);
  ASSERT_EQ(102, deletedValues_[1]);
}

TEST_F(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);
  Insert(300, 301);
  Entry* h = cache_->Lookup(EncodeKey(300));

  // Frequently used entry must be kept around,
  // as must things that are still in use.
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(301, Lookup(300));
  cache_->Release(h);
}

TEST_F(CacheTest, UseExceedsCacheSize) {
  // Overfill the cache, keeping handles on all inserted entries.
  std::vector<Entry*> h;
  for (int i = 0; i < kCacheSize + 100; i++) {
    h.push_back(InsertAndReturnEntry(1000 + i, 2000 + i));
  }

  // Check that all the entries can be found in the cache.
  for (int i = 0; i < h.size(); i++) {
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
  }

  for (int i = 0; i < h.size(); i++) {
    cache_->Release(h[i]);
  }
}

TEST_F(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2 * kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000 + i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}
