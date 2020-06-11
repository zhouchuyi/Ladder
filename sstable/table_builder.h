#pragma once


#include <string>
#include <string_view>
#include <memory>
#include <cassert>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "block_builder.h"

class TableBuilder
{
private:
    static constexpr uint64_t kDefaultBlockSize = 4 * 1024;
    std::string fileName_;
    std::string lastKey_;
    int fd_;

    uint64_t blockSize_;
    uint64_t entriesNum_;
    std::unique_ptr<KVBlockBuilder> kvBuilder_;
    std::unique_ptr<IndexBlockBuilder> IndexBuilder_;

    int openNewFile()
    {
        int fd = ::open(fileName_.c_str(),O_CLOEXEC | O_CREAT | O_TRUNC | O_RDWR,0644);
        assert(fd >= 0);
	    return fd;
    }
    std::pair<size_t,size_t> WriteBlock()
    {
        assert(fd_ >= 0);
        std::pair<size_t,size_t> sizeAndOffset;
        std::string_view content = kvBuilder_->Finish();
        sizeAndOffset.second = content.size();
        sizeAndOffset.first = lseek(fd_,0,SEEK_END);
        int res = ::write(fd_,content.data(),content.size());
        assert(res == content.size());
        ::fsync(fd_);
        return sizeAndOffset;
    }

public:
    TableBuilder(std::string fileName, uint64_t blockSize = kDefaultBlockSize)
     : fileName_(std::move(fileName)),
        fd_(openNewFile()),
        blockSize_(blockSize),
        entriesNum_(0),
        kvBuilder_(std::make_unique<KVBlockBuilder>()),
        IndexBuilder_(std::make_unique<IndexBlockBuilder>())
    {

    }
    ~TableBuilder()
    {
        if(fd_ != -1)
	        ::close(fd_);
    }

    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator= (const TableBuilder&) = delete;

    void Add(const std::string_view& key,const std::string_view& value);
    

    int Finish()
    {
        if(!kvBuilder_->empty())
        {
            auto res = WriteBlock();
            kvBuilder_->Reset();
            IndexBuilder_->Add(lastKey_,res);
        }
        
        std::string_view content = IndexBuilder_->Finish();
        size_t indexDataOffset = lseek(fd_,0,SEEK_END);
        ssize_t haswrite = ::write(fd_,content.data(),content.size());
        haswrite += ::write(fd_,&indexDataOffset,sizeof(size_t));
        assert(haswrite == content.size() + sizeof(size_t));
        IndexBuilder_->Reset();
        int ret = ::fsync(fd_);
        assert(ret == 0);
        ret = ::close(fd_);
        fd_ = -1;
        assert(ret == 0);
        return 0;
    }

    uint64_t NumEntries() const
    {
        return entriesNum_;
    }

    uint64_t FileSize() const
    {
        return lseek(fd_,0,SEEK_END);
    }

    std::string FileName() const { return fileName_; }

};

