#pragma once

#include <atomic>
#include <utility>
#include <memory>
#include <ctime>
#include <cstdlib>
#include <cassert>

#include "../util/IteratorBase.h"

template<typename KeyType,typename ValueType,typename Fn>
class SkipList
{
private:
    struct Node
    {
        Node(KeyType&& key,ValueType&& value)
         : key_(std::move(key)),
           value_(std::move(value))
        {

        } 
        const KeyType& key() const
        {
            return key_;
        }
        const ValueType& value() const
        {
            return value_;
        }
        void setNextRelaxed(uint16_t level,Node* node)
        {
            next_[level].store(node,std::memory_order_relaxed);
        }
        void setNext(uint16_t level,Node* node)
        {
            next_[level].store(node,std::memory_order_release);
        }
        Node* NextRelaxed(uint16_t level)
        {
            return next_[level].load(std::memory_order_relaxed);
        }
        Node* Next(uint16_t level)
        {
            return next_[level].load(std::memory_order_acquire);
        }

        KeyType   key_;
        ValueType value_;
        std::atomic<Node*> next_[1];   
    };
    
    Fn compare_;
    uint16_t currentHeight_;
    static constexpr uint16_t kDefaultMaxHeight = 12;
    Node* head_;

    bool lessThan(const KeyType& l,const KeyType& r) const
    {
        return compare_(l,r) == 1;
    }

    Node* findEqualOrGrater(const KeyType& key,Node** prev)
    {
        Node* current = head_;
        uint16_t level = currentHeight_ - 1;
        while (true)
        {
            Node* next = current->Next(level);
            //compare_ should return 1 if lhs < rhs
            if(next != nullptr && lessThan(next->key(),key))
            {
                current = next;
            }
            else
            {
                if(prev)
                    prev[level] = current;
                if(level == 0)
                {
                    return next;
                } 
                level--;
            }   
        }
        
    }

    Node* findLessThan(const KeyType& key) const
    {
        uint16_t level = currentHeight_;
        Node* current = head_;
        while (true)
        {
            Node* next = current->Next(level);
            if(next != nullptr && lessThan(next->key(),key))
            {
                current = next;
            }                
            else
            {
                if(level == 0)
                    return current;
                else
                {
                    level--;
                }
            }
            
        }
                   
    }

    Node* newNode(KeyType&& key,ValueType&& value,uint16_t height)
    {
        size_t mallocSize = sizeof(Node) + (height - 1) * sizeof(std::atomic<Node*>);
        char* node = static_cast<char*>(std::malloc(mallocSize));
        return new (node) Node(std::move(key),std::move(value));
        
    }

    int16_t randomHeight()
    {
        static constexpr uint16_t kBranching = 4;
        int height = 1;
        while (height < kDefaultMaxHeight && std::rand() % kBranching == 0)
            height++;
        return height;
    }

    Node* findLast() 
    {
        int level = currentHeight_ - 1;
        Node* current = head_;
        while (true)
        {
            Node* next = current->Next(level);
            if(next == nullptr)
            {
                if(level == 0)
                    break;
                else
                {
                    level--;
                }
                
            } else
            {
                current = next;
            }
            
        }
        return current;
    }

public:
    
    SkipList(Fn compare) 
     : compare_(std::move(compare)),
       currentHeight_(1),
       head_(nullptr)
    {
        head_ = newNode(KeyType{},ValueType{},kDefaultMaxHeight);
        for (size_t i = 0; i < kDefaultMaxHeight; i++)
        {
            head_->setNext(i,nullptr);
        }
        
        std::srand(std::time(nullptr));
    }

    ~SkipList()
    {
        
    }

    bool insert(KeyType key,ValueType value)
    {
        int height = randomHeight();
        Node* prev[kDefaultMaxHeight];
        Node* res = findEqualOrGrater(key,prev);   
        if(res != nullptr && compare_(res->key(),key) == 0)
            return false; 
        Node* node = newNode(std::move(key),std::move(value),height);
        if(height > currentHeight_)
        {
            for (uint16_t i = currentHeight_; i < height; i++)
            {
                prev[i] = head_;
            }
            currentHeight_ = height;
        }

        //insert to list at every level
        for (uint16_t i = 0; i < height; i++)
        {
            node->setNextRelaxed(i,prev[i]->NextRelaxed(i));
            prev[i]->setNext(i,node);
        }
        return true;
    }

    std::pair<bool,ValueType> find(const KeyType& key)
    {
        Node* node = findEqualOrGrater(key,nullptr);
        if(node == nullptr || node->key() != key)
        {
            return std::pair<bool,ValueType>(false,ValueType{});
        }else
        {
            return std::pair<bool,ValueType>(true,node->value());
        }
        
    }

    
    class Iterator 
    {
    private:
        SkipList* list_;
        Node*    node_;
    public:
        Iterator(SkipList* list,Node* node)
         : list_(list),
           node_(node)
        {

        }
        Iterator(const Iterator&) = default;
        ~Iterator() = default;
        bool operator == (const Iterator& it) const
        {
            return (it.list_ == list_) && (it.node_ == node_);
        }
        bool operator != (const Iterator& it) const
        {
            return !(*this == it);
        }
        void Next()
        {
            node_ = node_->Next(0);
        }

        void Prev()
        {
            assert(Valid());
            node_ = list_->findLessThan(node_->key());
            if(node_ == list_->head_)
                node_ = nullptr;
        }    
        const KeyType& key() const
        {
            return node_->key();
        }
        bool Valid() const
        {
            return node_ != nullptr;
        }

        const ValueType& value() const 
        {
            return node_->value();
        }
    
        void Seek(const KeyType& key)
        {
            node_ = list_->findEqualOrGrater(key,nullptr);
        }

        void SeekForLast()
        {
            node_ = list_->findLast();
            if(node_ == list_->head_)
                node_ = nullptr;
        }

        void SeekForForst()
        {
            node_ = list_->head_->Next(0);
        }
    };
    
    Iterator begin()
    {
        Node* node = head_;
        return Iterator(this,node->Next(0));
    }

    Iterator end()
    {
        return Iterator(this,nullptr);
    }

};
