#pragma once

#include "skiplist.h"
#include "../util/Compare.h"
#include "../util/IteratorBase.h"
#include "../util/InternalKey.h"
#include <string_view>

class MemTable
{
private:
    using KVSkipList = SkipList<InternalKey,std::string,InternalKeyComparator>;
    KVSkipList storage_;     
    class IteratorImpl;
public:
    MemTable()
     : storage_(InternalKeyComparator{})
    {
        
    }
    
    ~MemTable() = default;

    MemTable(const MemTable&) = delete;

    MemTable& operator = (const MemTable&) = delete;

    static std::shared_ptr<MemTable> newMemTable()
    {
        return std::make_shared<MemTable>();
    }

    void Add(std::string_view key,std::string_view value,SequenceNumber seq);

    bool Get(std::string_view key,std::string& value);
    
    IteratorBase<InternalKey,std::string_view>* newIterator();
    
};

