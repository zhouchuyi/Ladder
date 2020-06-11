#pragma once

#include "table.h"
#include "../util/LRUCache.h"


class TableCache
{
private:
    Entry* findTable(uint64_t fileNumber,uint64_t fileSize);
    const std::string dbname_;
    ShardedLRUCache* cache_;
public:
    TableCache(const std::string& dbname,int entries);
    ~TableCache();

    std::shared_ptr<SSTable::Iterator> 
            NewIterator(uint64_t fileNumber,uint64_t fileSize,std::shared_ptr<SSTable>& ptr)
    {
        Entry* entry_ = findTable(fileNumber,fileSize);
        if(entry_ == nullptr)
            return nullptr;
        SSTable* table = reinterpret_cast<SSTable*>(entry_->value_);
        auto cleaner = [entry = entry_,cache = cache_](){
            cache->Release(entry);
        };
        std::shared_ptr<SSTable::Iterator> it(table->newIterator(),std::move(cleaner));
        return it;
    }

    template<typename F>
    bool Get(uint64_t fileNumber,uint64_t fileSize,const std::string_view& k,F handle)
    {
        Entry* entry = findTable(fileNumber,fileSize);
        if(entry == nullptr)
            return false;
        SSTable* table = reinterpret_cast<SSTable*>(entry->value_);
        table->InternalGet(k,std::move(handle));
        cache_->Release(entry);
    }

    void Evict(uint64_t fileNumber)
    {
        char buf[sizeof(fileNumber)];
        memcpy(buf,&fileNumber,sizeof(fileNumber));
        cache_->Erase(std::string(buf));
    }

};

