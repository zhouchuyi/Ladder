#pragma once

#include <string_view>
#include <vector>
#include <memory>

#include "../util/IteratorBase.h"
#include "table.h"

class MergeIterator : public IteratorBase<std::string_view,std::string_view>
{
private:

    enum class DIRECTION { FROWARD,REVERSE };

    void findLargest()
    {
        assert(!itList_.empty());
        auto it = itList_.begin();
        std::string_view key = (*it)->key();
        current_ = *it;
        while (it != itList_.end())
        {
            //key < it->key()
            if(compare_(key,(*it)->key()) == 1)
            {
                key = (*it)->key();
                current_ = *it;
            }
            it++;
        }
    }

    void findSmallest()
    {
        assert(!itList_.empty());
        auto it = itList_.begin();
        std::shared_ptr<Iterator> smallest = nullptr;
        while (it != itList_.end())
        {
            if((*it)->Valid())
            {
                if(smallest == nullptr)
                {
                    smallest = *it;
                } else if (compare_(smallest->key(),(*it)->key()) == -1)
                {
                    //smallest->key() > (*it)->key()
                    smallest = *it;
                }
                
            }
            it++;
        }
        current_ = smallest;
    }

public:

    using Iterator = SSTable::Iterator;
    using IteratorList = std::vector<std::shared_ptr<Iterator>>;
    
    MergeIterator(IteratorList&& tablesIterators)
     : itList_(std::move(tablesIterators)),
       current_(nullptr)
    {

    }

    ~MergeIterator() = default;

    bool Valid() const override
    {
        return current_ != nullptr;
    }


    void SeekForFirst() override
    {
        for (const auto & it : itList_)
        {
            it->SeekForFirst();
        }
        findSmallest();
        direction_ = DIRECTION::FROWARD;
    }

    void SeekForLast() override
    {
        for (const auto & it : itList_)
        {
            it->SeekForLast();
        }
        findLargest();        
        direction_ = DIRECTION::REVERSE;
    }

    //TODO
    void Seek(const std::string_view& target) override
    {
        for (auto & it : itList_)
        {
            it->Seek(target);
        }
        findSmallest();
        direction_ = DIRECTION::FROWARD;
    }

    //TODO
    void Next() override
    {
        assert(Valid());
       
        current_->Next();
        findSmallest();
    }

    void Prev() override
    {   assert(Valid());
        
        
        current_->Prev();
        findLargest();
    }

    std::string_view key() const override
    {
        assert(current_);
        return current_->key();
    }

    std::string_view value() const override
    {
        assert(current_);
        return current_->value();
    }

private:

    IteratorList itList_;
    std::shared_ptr<Iterator> current_;

    InternalKeyStringViewComparator compare_{};

    DIRECTION direction_;
};

