#pragma once

#include <string>
#include <memory>
#include <iostream>
#include "./block.h"
#include "../util/LRUCache.h"


class SSTable
{
private:

    InternalKeyUserComparator userComparator_;

    std::unique_ptr<IndexBlock> indexBlock_{nullptr};
    //KVBlock cache
    std::shared_ptr<ShardedLRUCache> cache_{nullptr};

    bool opened_{false};

    std::string fileName_{};

    size_t indexDataoffset_{0};

    size_t totalSize_{0};
    
    int fd_{-1};

    void loadIndexblock();
    
    BlockContent loadKVBlock(const std::pair<size_t,size_t>& location);

    std::shared_ptr<KVIterator> KVBlockReader(const std::pair<size_t,size_t>& location);
    
    void InsertKVBlockToCache(std::string key,KVBlock* block)
    {
        Entry* entry = cache_->Insert(key,block,1,&SSTable::KVBlockDestroy);
        cache_->Release(entry);
    }

    std::string pairToString(const std::pair<size_t,size_t>& value) const
    {
        return std::to_string(value.first) + std::to_string(value.second);
    }

    static void KVBlockDestroy(const std::string& key,void* value)
    {
        KVBlock* block = static_cast<KVBlock*>(value);
        delete block;
    } 

    class IteratorImpl;

public:
    SSTable() = default;
    ~SSTable();


    void OpenTable(const std::string& filename)
    {
        fileName_ = filename;
        loadIndexblock();
        //TODO
        cache_ = ShardedLRUCache::NewCache(64);
    }

    static std::shared_ptr<SSTable> newTable(const std::string& filename)
    {
        auto table = std::make_shared<SSTable>();
        table->OpenTable(filename);
        return table;
    }

    static SSTable* newTableRaw(const std::string& filename)
    {
        SSTable* table = new SSTable;
        table->OpenTable(filename);
        return table;
    }

    SSTable(const SSTable&) = delete;

    SSTable& operator = (const SSTable&) = delete;
    
    template<typename F>
    int InternalGet(const std::string_view& key,F&& handle)
    {
        assert(opened_);
        bool find = false;
        IndexIterator* iit = indexBlock_->newIterator();
        iit->Seek(key);
        if(iit->Valid())
        {
            std::shared_ptr<KVIterator> kvit = KVBlockReader(iit->value());
            kvit->Seek(key);
            std::string_view ikey = kvit->key();
            if(userComparator_(ikey,key) == 0)
            {
                find = true;
                handle(kvit->key(),kvit->value());
            }
        }
        delete iit;
        return find ? 0 : -1;
    }
   
    bool isOpen() const
    {
        return opened_;
    }

    size_t totalSize() const
    {
        return totalSize_;
    }

    using Iterator = IteratorBase<std::string_view,std::string_view>;
    
    Iterator* newIterator();
};

