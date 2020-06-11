#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <cstring>
#include <iostream>
#include "../util/Compare.h"
#include "../util/IteratorBase.h"


struct BlockContent
{
    const char* data_;
    uint64_t size_;
    bool owned_;
};


template<typename K,typename V,typename Comparator> 
class Block 
{
private:
    
    Comparator compare_;
    BlockContent content_;
    uint32_t restartNum_;
    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    uint32_t readNumRestarts()
    {
        size_t offset = content_.size_ - sizeof(uint32_t);
        char* start = const_cast<char*>(content_.data_ + offset);
        uint32_t num = 0;
        memcpy(&num,start,sizeof(uint32_t));
        return num;
    }

public:

    class Iterator;

    explicit Block(BlockContent&& content,Comparator comparator)
     : content_(std::move(content)),
       compare_(std::move(comparator)),
       restartNum_(readNumRestarts())
    {

    }

    ~Block()
    {
        if(content_.owned_)
        {
            char* data = const_cast<char*>(content_.data_);
            free(static_cast<void*>(data));
        }    
    }

    size_t size() const
    {
        return content_.size_;
    }    

    uint32_t NumRestarts() const
    {
        return restartNum_;
    }

    IteratorBase<K,V>* newIterator() ;

   

};

// using KVBlock = Block<std::string_view,std::string_view,StringViewComparator>;

// using IndexBlock = Block<std::string_view,std::pair<size_t,size_t>,StringViewComparator>;

using KVBlock = Block<std::string_view,std::string_view,InternalKeyStringViewComparator>;

using IndexBlock = Block<std::string_view,std::pair<size_t,size_t>,InternalKeyStringViewComparator>;

using KVIterator = IteratorBase<std::string_view,std::string_view>;

using IndexIterator = IteratorBase<std::string_view,std::pair<size_t,size_t>>;
