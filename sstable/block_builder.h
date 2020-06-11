#pragma once

#include <iostream>
#include <cassert>
#include <vector>
#include <string_view>
#include <string>

#include "../util/format.h"

template<typename V>
class BlockBuilder
{
private:
    std::string buffer_;
    std::string lastkey_;
    int interval_;
    size_t size_;

    std::vector<uint32_t> restarts_;
    bool finished_;
    static constexpr int kDefaultInterval = 16;
public:
    BlockBuilder(int blockStartInterval = kDefaultInterval)
    : buffer_(),
      lastkey_(),
      interval_(blockStartInterval),
      size_(0),
      restarts_(),
      finished_(false)
    {

    }

    ~BlockBuilder()
    {
        Reset();
    }

    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder& operator=(const BlockBuilder&) = delete;

    void Add(const std::string_view& key,const V& value);
    
    
    void Reset()
    {
        buffer_ = std::string{};
        lastkey_ = std::string{};
        restarts_.clear();
        size_ = 0;
    }

    std::string_view Finish()
    {
        for (auto & restart : restarts_)
        {
            AppendUINT32(buffer_,restart);
        }
        AppendUINT32(buffer_,restarts_.size());
        finished_ = true;
        return std::string_view(buffer_.data(),buffer_.size());
    }

    bool empty() const
    {
        return buffer_.empty();
    }

    uint64_t CurrentSize() const
    {
        return buffer_.size() + 
                restarts_.size() * sizeof(uint32_t) + 
                    sizeof(uint32_t);
    }
};


using KVBlockBuilder = BlockBuilder<std::string_view>;

using IndexBlockBuilder = BlockBuilder<std::pair<size_t,size_t>>;


