#pragma once


template<typename K,typename V>
class IteratorBase
{
private:

public:
    IteratorBase() = default;

    IteratorBase(const IteratorBase&) = delete;
    IteratorBase& operator=(const IteratorBase&) = delete;
    
    virtual ~IteratorBase() = default;

    virtual bool Valid() const = 0;

    virtual void SeekForFirst() = 0;

    virtual void SeekForLast() = 0;

    virtual void Seek(const K&) = 0;

    virtual void Next() = 0;

    virtual void Prev() = 0;

    virtual K key() const = 0;

    virtual V value() const = 0;

};
