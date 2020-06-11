#include "block.h"

#include <cassert>
#include <iostream>

using KeyLengType = uint32_t;
static constexpr size_t LengthStore = sizeof(KeyLengType);
using ValueLengType = KeyLengType;

using RestartType = uint32_t;
static constexpr size_t RestartStore = sizeof(RestartType); 

static KeyLengType readKeyLength(const char* data)
{
    uint8_t* begin = reinterpret_cast<uint8_t*>(const_cast<char*>(data));
    KeyLengType len = *reinterpret_cast<KeyLengType*>(begin);
    return len;
}

static std::string_view readKey(const char* data)
{
    KeyLengType len = readKeyLength(data);    
    char* begin = const_cast<char*>(data + LengthStore);
    return std::string_view(begin,len);
}


static const char* locateWithRestartIndex(uint32_t index,uint32_t restartNum,uint64_t size,const char* data)
{
    const char* begin = data + 
                            (size - (restartNum - index) * RestartStore - sizeof(uint32_t)); 

    RestartType offset = *reinterpret_cast<RestartType*>(const_cast<char*>(begin));
    return data + offset;

}



//IndexBlock
template<>
class IndexBlock::Iterator : public IteratorBase<std::string_view,std::pair<size_t,size_t>>
{
private:

    Block* block_;
    const char* data_;
    uint32_t restartsNum_;
    uint32_t currentIndex_;
    uint32_t restartIndex_;
    uint64_t size_;
    std::string_view key_;
    std::pair<size_t,size_t> value_;

    std::string_view keyForRestartPoint(int index)
    {
        const char* keyBegin = locateWithRestartIndex(index,restartsNum_,size_,data_);
        std::string_view key = readKey(keyBegin);
        return key;
    }
    std::pair<size_t,size_t> valueForKey(std::string_view key)
    {
        std::pair<size_t,size_t> value;
        const char* keyBegin = key.data();
        const char* valueBegin = keyBegin + key.size();
        value.first = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin));
        value.second = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin) + sizeof(size_t));
        return value;

    }
public:
    Iterator(Block* block,const char* data,uint32_t restartsNum,uint64_t size)
     : block_(std::move(block)),
       data_(data),
       size_(size),
       currentIndex_(0),
       restartIndex_(0),
       restartsNum_(restartsNum)
    {
        
    }

    ~Iterator() override
    {

    }

    bool Valid() const override
    {
        return (currentIndex_ >= 0) && (currentIndex_ < restartsNum_);
    }

    void SeekForFirst() override
    {
        currentIndex_ = 0;
        const char* keyBegin = data_;
        key_ = readKey(keyBegin);
        const char* valueBegin = keyBegin + key_.length() + LengthStore;
        value_.first = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin));
        value_.second = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin) + sizeof(size_t));
    }

    //binary search
    void SeekForLast() override
    {
        const char* keyBegin = locateWithRestartIndex(restartsNum_ - 1,restartsNum_,size_,data_);
        key_ = readKey(keyBegin);
        const char* valueBegin = keyBegin + key_.length() + LengthStore;
        value_.first = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin));
        value_.second = *reinterpret_cast<size_t*>(const_cast<char*>(valueBegin) + sizeof(size_t));
        currentIndex_ = restartsNum_ - 1;
    }

    //binary search return key >= target
    void Seek(const std::string_view& target) override
    {
        int l = 0;
        int r = restartsNum_ - 1;
        bool find = false;
        std::string_view key;
        while (l <= r)
        {
            int mid = l + (r - l) / 2;
            key = keyForRestartPoint(mid);
            // mid == target 
            if(block_->compare_(key,target) == 0)
            {
                l = mid;
                find = true;
                break;
            } else if (block_->compare_(key,target) == 1)
            {
                //key < target
                l = mid + 1;
            } else
            {
                //key > target
                r = mid - 1;
            }
        }

        if(!find)
        {
            key = keyForRestartPoint(l); 
        }  

        key_ = key;
        currentIndex_ = l;
        value_ = valueForKey(key_);
       
        //invalid key_ < target
        if(block_->compare_(key_,target) == 1)
        {
            currentIndex_ = -1;
        }

    }

    void Next() override
    {
        if(++currentIndex_ < restartsNum_)
        {
            key_ = keyForRestartPoint(currentIndex_);
            value_ = valueForKey(key_);
        }
    }

    void Prev() override
    {
        if(--currentIndex_ >= 0)
        {
            key_ = keyForRestartPoint(currentIndex_);
            value_ = valueForKey(key_);
        }
    }

    std::string_view key() const override
    {
        return key_;
    }

    std::pair<size_t,size_t> value() const override
    {
        return value_;
    }
};

//KVBlock
template<>
class KVBlock::Iterator : public IteratorBase<std::string_view,std::string_view>
{
private:
    Block* block_;
    const char* data_;
    uint64_t restartsOffset_;
    uint32_t restartsNum_;
    uint32_t currentOffset_;
    uint32_t currentIndex_;
    
    uint64_t size_;
    std::string_view key_;
    std::string_view value_;

    void updateIndex(uint32_t hint)
    {
        std::string_view hintKey = readKey(locateWithRestartIndex(hint,restartsNum_,size_,data_)); 
        //hintKey <= key_
        if(block_->compare_(hintKey,key_) >= 0)
        {
            //do update
            currentIndex_ = hint;
        }        
    }

    std::string_view valueForKey(std::string_view key)
    {
        const char* keyBegin = key.data();    
        const char* valueLengthBegin = keyBegin + key.length();
        ValueLengType length = *reinterpret_cast<ValueLengType*>(const_cast<char*>(valueLengthBegin));
        const char* valueBegin = valueLengthBegin + LengthStore;
        return std::string_view(valueBegin,length);
    }

    bool praseNextEntry()
    {
        assert(currentOffset_ <= restartsOffset_);
        if(currentOffset_ == restartsOffset_)
        {
            return false;
        }
        currentOffset_ += key_.length() + value_.length() + 2 * LengthStore;
        key_ = readKey(data_ + currentOffset_);
        value_ = valueForKey(key_);
        assert(currentOffset_ <= restartsOffset_);
        return currentOffset_ != restartsOffset_;
    }
public:

    Iterator(Block* block,const char* data,uint32_t restartsNum,uint64_t restartsOffset,uint64_t size)
     : block_(block),
       data_(data),
       restartsOffset_(restartsOffset),
       currentOffset_(0),
       restartsNum_(restartsNum),
       size_(size)
    {

    }

    ~Iterator() override
    {

    }

    bool Valid() const override
    {
        return (currentOffset_ >= 0) && (currentOffset_ < restartsOffset_);
    }

    void SeekForFirst() override
    {
        currentOffset_ = 0;
        const char* begin = locateWithRestartIndex(0,restartsNum_,size_,data_);
        key_ = readKey(begin);
        value_ = valueForKey(key_);
        currentIndex_ = 0;
    }

    void SeekForLast() override
    {
        char* begin = const_cast<char*>(locateWithRestartIndex(restartsNum_ - 1,restartsNum_,size_,data_));
        currentOffset_ = begin - data_;
        while (currentOffset_ < restartsOffset_)
        {
            key_ = readKey(begin);
            value_ = valueForKey(key_);
            currentOffset_ += key_.length() + value_.length() + LengthStore * 2;
            begin = const_cast<char*>(data_ + currentOffset_);
        }
        assert(currentOffset_ == restartsOffset_);
        currentOffset_ -= key_.length() + value_.length() + 2 * LengthStore;
        currentIndex_ = restartsNum_ - 1;
    }

    void Seek(const std::string_view& target) override
    {
        int l = 0;
        int r = restartsNum_ - 1; 
        bool find = false;
        std::string_view key;
        while (l <= r)
        {
            int mid = l + (r - l) / 2;
            key = readKey(locateWithRestartIndex(mid,restartsNum_,size_,data_));
            if(block_->compare_(key,target) == 0)
            {
                r = mid;
                find = true;
                break;
            } else if (block_->compare_(key,target) == 1)
            {
                //key < target
                l = mid + 1;
            } else
            {
                //key > target
                r = mid - 1;
            }
        }
        if(r == -1)
            r = 0;
        currentIndex_ = r;
        if(find)
        {
            key_ = key;
            value_ = valueForKey(key_);
            currentOffset_ = key_.data() - data_ - LengthStore;
            return;
        }else
        {
            key = readKey(locateWithRestartIndex(r,restartsNum_,size_,data_));
            key_ = key;
            value_ = valueForKey(key_);
            currentOffset_ = key_.data() - data_ - LengthStore;
        }
        do
        {
            //key >= target
            if(block_->compare_(key_,target) <= 0)
            {
                return;
            }
            if(!praseNextEntry())
                return;
        } while (true);
        
    }

    void Next() override
    {
        if(currentOffset_ + 2 * LengthStore + key_.length() + value_.length() >= restartsOffset_)
        {
            currentOffset_ += 2 * LengthStore + key_.length() + value_.length();
            return;
        } else if (currentOffset_ == restartsOffset_)
        {
            return;
        }
        
        if(key_.empty())
        {
            assert(value_.empty());
            currentOffset_ = 0;
        } else
        {
            currentOffset_ = key_.data() - data_ - LengthStore;    
        }
        praseNextEntry();
        if(currentIndex_ != restartsNum_ - 1)
            updateIndex(currentIndex_ + 1);
    }

    //todo
    void Prev() override
    {
        if(currentOffset_ == 0)
        {
            currentOffset_ = -1;
            return;
        }
        std::string_view key = readKey(locateWithRestartIndex(currentIndex_,restartsNum_,size_,data_));
        std::string_view target = key_;
        //key <= key_
        assert(block_->compare_(key,key_) >= 0);
        if(block_->compare_(key,key_) == 0)
        {
            assert(currentIndex_ >= 1);
            key = readKey(locateWithRestartIndex(currentIndex_ - 1,restartsNum_,size_,data_));
            currentIndex_ -= 1;
        }
        key_ = key;
        currentOffset_ = key_.data() - data_ - LengthStore;
        while (block_->compare_(key_,target) != 0)
        {
            key = key_;
            praseNextEntry();
        }
        currentOffset_ = key.data() - data_ - LengthStore;
        key_ = key;
        value_ = valueForKey(key_);

    }

    std::string_view key() const override
    {
        return key_;
    }

    std::string_view value() const override
    {
        return value_;
    }

};



template<>
IndexIterator* IndexBlock::newIterator() 
{
    return new Iterator(this,content_.data_,restartNum_,content_.size_);
}


template<>
KVIterator* KVBlock::newIterator()
{
    uint64_t restartOffset = content_.size_ - restartNum_ * RestartStore - LengthStore;
    return new Iterator(this,content_.data_,restartNum_,restartOffset,content_.size_);
}
