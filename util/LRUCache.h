#pragma once

#include <unordered_map>
#include <string>
#include <mutex>
#include <memory>
#include <cassert>
#include <cstring>
#include <iostream>
struct Entry
{
    void* value_;
    void (*deleter_)(const std::string&,void* value);
    Entry* next_;
    Entry* prev_;
    size_t charge_;
    size_t keyLength_;
    size_t refs_;
    bool inCache_;
    char data_[1];

    std::string key() const
    {
        return std::string(data_,keyLength_);
    }
};
class LRUCache
{
public:
    using HashTable = std::unordered_map<std::string,Entry*>;
    LRUCache()
    {
        activeList_.next_ = &activeList_;
        activeList_.prev_ = &activeList_;

        inactiveList_.next_ = &inactiveList_;
        inactiveList_.prev_ = &inactiveList_;
    }
    ~LRUCache()
    {
        assert(activeList_.next_ = &activeList_);
        for (Entry* e = inactiveList_.next_; e != &inactiveList_;)
        {
            
            e->inCache_ = false;
            if(e->refs_ != 1)
            {
                // std::cout << e->refs_ <<" "<<e->key() <<std::endl;
                assert(false);
            }

            
            UnRef(e);
            e = e->next_; 
        }
        
    }

    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    Entry* Insert(const std::string& key,void* value,size_t charge,void (*deleter)(const std::string& key,void* value))
    {
        Entry* entry = static_cast<Entry*>(malloc(sizeof(Entry) + key.length()));
        entry->value_ = value;
        entry->deleter_ = deleter;
        entry->charge_ = charge;
        entry->keyLength_ = key.length();
        entry->inCache_ = true;
        entry->refs_ = 2;
        memcpy(entry->data_,key.data(),key.length());
        usage_ += charge;

        {
            std::lock_guard<std::mutex> lk(mutex_);
            FinishErase(table_.find(key));
            LRUAppend(&activeList_,entry);
            table_[key] = entry;
            while (usage_ > capacity_ && inactiveList_.next_ != &inactiveList_)
            {
                Entry* old = inactiveList_.next_;  
                HashTable::iterator it = table_.find(old->key());                                   
                FinishErase(it);
                table_.erase(it);
            }   
        }
        return entry;
    }

    Entry* LookUp(const std::string& key)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        HashTable::iterator it = table_.find(key);
        if(it != table_.end())
        {
            Ref(it->second);
        }
        return it == table_.end() ? nullptr : it->second;
    }

    void Release(Entry* e)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        // std::cout << "Release " << e->key() <<" " << reinterpret_cast<uintptr_t>(e->value_) <<" "<<e->refs_<<std::endl;            
        UnRef(e);
    }

    void* Value(const std::string& key)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        HashTable::iterator it = table_.find(key);
        return it == table_.end() ? nullptr : it->second->value_;
    }

    void Erase(const std::string& key)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        HashTable::iterator it = table_.find(key);
        if(it == table_.end())
            return;
        FinishErase(it);
        table_.erase(it);
    }

    void Prune()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        while (inactiveList_.next_ != &inactiveList_)
        {
            Entry* e = inactiveList_.next_;
            HashTable::iterator it = table_.find(e->key());
            assert(FinishErase(it));
            table_.erase(it);
        }
        
    }

    size_t TotalCharge() const
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return usage_;
    }

    void setCapacity(size_t capacity) 
    {
        std::lock_guard<std::mutex> lk(mutex_);
        capacity_ = capacity;
    }

    size_t Capacity() const
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return capacity_;
    }

private:
    void LRUAppend(Entry* list,Entry* e)
    {   
        // entry->prev_ = list->prev_;
        // entry->next_ = list;
        // list->prev_->next_ = entry;
        // list->prev_ = entry;
        e->next_ = list;
        e->prev_ = list->prev_;
        e->prev_->next_ = e;
        e->next_->prev_ = e;
    }

    void LRURemove(Entry* entry)
    {
        entry->prev_->next_ = entry->next_;
        entry->next_->prev_ = entry->prev_;

        entry->next_ = nullptr;
        entry->prev_ = nullptr;
    }

    void Ref(Entry* e)
    {
        if(e->refs_ == 1)
        {
            assert(e->inCache_);
            LRURemove(e);
            LRUAppend(&activeList_,e);
        }
        // std::cout << "Ref " << e->key() <<" " << reinterpret_cast<uintptr_t>(e->value_) <<" "<<e->refs_<<std::endl;            
        e->refs_++;
    }

    void UnRef(Entry* e)
    {
        e->refs_--;
        if(e->refs_ == 0)
        {
            assert(!e->inCache_);
            // usage_ -= e->charge_;
            e->deleter_(std::string(e->data_,e->keyLength_),e->value_);
            free(e);
        } else if (e->refs_ == 1 && e->inCache_)
        {
            assert(e->inCache_);
            LRURemove(e);
            LRUAppend(&inactiveList_,e);
        }
    }

    bool FinishErase(HashTable::iterator it)
    {
        if(it != table_.end())
        {
            Entry* entry = it->second;
            usage_ -= entry->charge_;
            LRURemove(entry);
            entry->inCache_ = false;
            
            
            UnRef(entry);
        }
        return it != table_.end();
    }

private:
    
    size_t usage_;

    size_t capacity_;

    Entry activeList_;

    Entry inactiveList_;
    
    HashTable table_;

    mutable std::mutex mutex_;
};

class ShardedLRUCache
{
private:

    static constexpr int kDefaultShardBits = 4;
    static constexpr int64_t kDefaultShardsNum = 1 << kDefaultShardBits; 

    LRUCache shards_[kDefaultShardsNum];
    std::hash<std::string> hash_;
public:
    ShardedLRUCache(size_t capacity)
    {
        size_t preShard = (capacity + kDefaultShardsNum - 1) / kDefaultShardsNum;
        for (auto & shard : shards_)
        {
            shard.setCapacity(preShard);
        }
    }

    ~ShardedLRUCache() = default;


    static std::shared_ptr<ShardedLRUCache> NewCache(size_t capacity)
    {
        return std::make_shared<ShardedLRUCache>(capacity);
    }

    uint32_t Shard(const std::string& key)
    {
        return hash_(key) % kDefaultShardsNum;
    }

    Entry* Insert(const std::string& key,void* value,size_t charge,void (*deleter)(const std::string& key,void* value))
    {
        uint32_t shard = Shard(key);
        return shards_[shard].Insert(key,value,charge,deleter);
    }
    Entry* Lookup(const std::string& key)
    {
        uint32_t shard = Shard(key);
        return shards_[shard].LookUp(key);
    }

    void Erase(const std::string& key)
    {
        uint32_t shard = Shard(key);
        return shards_[shard].Erase(key);
    }

    void Release(Entry* e)
    {
        uint32_t shard = Shard(e->key());
        shards_[shard].Release(e);
    }

    void* Value(const std::string& key)
    {
        uint32_t shard = Shard(key);
        return shards_[shard].Value(key);
    }
};


