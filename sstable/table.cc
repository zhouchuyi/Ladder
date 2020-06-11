#include "table.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

void SSTable::loadIndexblock()
{
    assert(!opened_);
    fd_ = ::open(fileName_.c_str(),O_CLOEXEC | O_RDONLY,0644);
    if(fd_ < 0)
    {
        perror("open file");
    } 
    assert(fd_ >= 0);
    totalSize_ = lseek(fd_,0,SEEK_END);      
    lseek(fd_,-sizeof(size_t),SEEK_END);
    ::read(fd_,&indexDataoffset_,sizeof(size_t));
    
    size_t IndexBlockSize = totalSize_ - indexDataoffset_ - sizeof(size_t);
    void* buffer = malloc(IndexBlockSize);
    assert(lseek(fd_,indexDataoffset_,SEEK_SET) == indexDataoffset_);
    assert(::read(fd_,buffer,IndexBlockSize) == IndexBlockSize);
    BlockContent indexContent{static_cast<char*>(buffer),IndexBlockSize,true};
    indexBlock_ = std::make_unique<IndexBlock>(std::move(indexContent),InternalKeyStringViewComparator{});
    opened_ = true;
}


SSTable::~SSTable()
{
    if(opened_)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

std::shared_ptr<KVIterator> SSTable::KVBlockReader(const std::pair<size_t,size_t>& value)
{
    std::string cacheKey = pairToString(value);
    Entry* entry_ = cache_->Lookup(cacheKey);  
    KVBlock* block = nullptr;
    if(entry_ != nullptr)
    {
        block = static_cast<KVBlock*>(entry_->value_);
    } else
    {
        BlockContent content = loadKVBlock(value);
        block = new KVBlock(std::move(content),InternalKeyStringViewComparator{});
        entry_ = cache_->Insert(pairToString(value),block,1,KVBlockDestroy);

    }
    std::shared_ptr<KVIterator> it = std::shared_ptr<KVIterator>(block->newIterator(),[cache = cache_,entry = entry_](KVIterator* it){
        cache->Release(entry);
        delete it;
    });
    return it;
}

BlockContent SSTable::loadKVBlock(const std::pair<size_t,size_t>& location)
{
    size_t offset = location.first;
    size_t size = location.second;
    void* buf = malloc(size);
    assert(lseek(fd_,offset,SEEK_SET) == offset);
    ssize_t hasRead = ::read(fd_,buf,size);
    assert(hasRead == size);
    BlockContent content{static_cast<char*>(buf),size,true};
    return content;
}


class SSTable::IteratorImpl : public IteratorBase<std::string_view,std::string_view>
{
private:

    SSTable* table_;

    std::shared_ptr<IndexIterator> IndexIt_;
    std::shared_ptr<KVIterator> KVIt_;
    std::pair<size_t,size_t> locationCache_;
   
    void updateKVIt()
    {
        if(locationCache_ == IndexIt_->value())
            return;
        //update new KVIterator
        locationCache_ = IndexIt_->value();
        KVIt_ = table_->KVBlockReader(locationCache_);
    }


    
public:

    IteratorImpl(SSTable* table)
     : table_(table),
       IndexIt_(table_->indexBlock_->newIterator()),
       KVIt_(nullptr),
       locationCache_(std::make_pair(0,0))
    {

    }

    ~IteratorImpl() override = default;

    bool Valid() const override
    {
        if(KVIt_ && IndexIt_)
        {
            return KVIt_->Valid() && IndexIt_->Valid();
        }
        return false;
    }

    void SeekForFirst() override
    {
        IndexIt_->SeekForFirst();
        updateKVIt();
        KVIt_->SeekForFirst();
    }

    void SeekForLast() override
    {
        IndexIt_->SeekForLast();
        updateKVIt();
        KVIt_->SeekForLast();
    }

    void Seek(const std::string_view& target) override
    {
        IndexIt_->Seek(target);

        assert(InternalKeyStringViewComparator{}(IndexIt_->key(),target) <= 0);

        updateKVIt();
        KVIt_->Seek(target);
    }

    void Next() override
    {
        assert(KVIt_);
        KVIt_->Next();
        if(KVIt_->Valid())
            return;
        
        //TODO 
        IndexIt_->Next();
        if(IndexIt_->Valid())
        {
            updateKVIt();
            KVIt_->SeekForFirst();
        }

    }

    void Prev() override
    {
        assert(KVIt_);
        KVIt_->Prev();
        if(KVIt_->Valid())
            return;
        //TODO
        
        IndexIt_->Prev();
        if(IndexIt_->Valid())
        {
            updateKVIt();
            KVIt_->SeekForLast();
        }
    }

    std::string_view key() const override
    {
        return KVIt_->key();
    }

    std::string_view value() const override
    {
        return KVIt_->value();
    }

};


SSTable::Iterator* SSTable::newIterator() 
{
    return new SSTable::IteratorImpl(this);
}
