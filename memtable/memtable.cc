#include "memtable.h"

#include <iostream>
void MemTable::Add(std::string_view key,std::string_view value,SequenceNumber seq)
{
    storage_.insert(InternalKey(key,seq,OpsType::UPDATE),std::string(value));
}

bool MemTable::Get(std::string_view key,std::string& value)
{
    InternalKey ikey(key,kDefaultMaxSequenceNumber,OpsType::UPDATE);
    auto it = newIterator();
    it->Seek(ikey);
    InternalKey findKey = it->key();
    if(findKey.ExtractUserKey() == key)
    {
        value = it->value();
        delete it;
        return true;
    } else
    {
        delete it;
        return false;
    }
  

}

class MemTable::IteratorImpl : public IteratorBase<InternalKey,std::string_view>
{
public:
   
    IteratorImpl(KVSkipList::Iterator it)
     : it_(it)
    {

    }

    ~IteratorImpl() override
    {
        
    }

    bool Valid() const override
    {
        return it_.Valid();
    }

    void SeekForFirst() override
    {
        it_.SeekForForst();
    }

    void SeekForLast() override
    {
        it_.SeekForLast();
    }

    void Seek(const InternalKey& key) override
    {
        it_.Seek(key);
    }

    void Next() override
    {
        it_.Next();
    }

    void Prev() override
    {
        it_.Prev();
    }

    InternalKey key() const override
    {
        return it_.key();
    }

    std::string_view value() const override
    {
        return it_.value();
    }

private:
    KVSkipList::Iterator it_;
};


IteratorBase<InternalKey,std::string_view>* 
MemTable::newIterator()
{
    return new IteratorImpl(storage_.begin());
}
