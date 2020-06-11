#include "log_manager.h"
#include "../util/CRC.h"

#include <cassert>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

std::string LogManager::kDefaultLogFileName = "defaultLog.log";
static constexpr size_t CheckSumSize = sizeof(uint32_t);
static constexpr size_t LengthStoreSize = sizeof(uint32_t);

static bool SafeMemcpy(char* buf,void* dest,size_t current,size_t wide,size_t size)
{
    if(current + wide > size)
        return false;
    char* begin = buf + current;
    memcpy(dest,begin,wide);
    return true;
}

int LogManager::LogReader::Open()
{
    bool exists = ::access(fileName_.c_str(), 0) == 0;
    if(exists == false)
    {
        return 1;
    }

    fd_ = ::open(fileName_.c_str(),O_RDONLY | O_CLOEXEC,0644);
    fileSize_ = lseek(fd_,0,SEEK_END);
    lseek(fd_,0,SEEK_SET);
    if(fd_ < 0)
    {
        return -1;
    }
    return 0;
}


std::vector<LogManager::LogReader::KVPair> 
LogManager::LogReader::LoadToEnd()
{
    return doRead(true);
}

std::vector<LogManager::LogReader::KVPair>  
LogManager::LogReader::doRead(bool isToEnd)
{
    assert(fd_ >= 0);
    lseek(fd_,0,SEEK_SET);
    char* buf = static_cast<char*>(malloc(fileSize_));
    ssize_t hasRead = ::read(fd_,buf,fileSize_);
    assert(hasRead == fileSize_);

    auto res = Decode(buf,fileSize_);
    free(buf);
    return res;    
    
}

LogManager::LogReader::~LogReader()
{
    if(fd_ != -1)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

void LogManager::LogWriter::Add(std::string_view key,std::string_view value)
{
    //CRC32|KEYLENGTH|KEY|VALUELENGTH|VALUE|
    std::string content = LogManager::Encode(key,value);
    waitingContents_.push_back(std::move(content));
    doWrite();
}

void LogManager::LogWriter::Add(const std::vector<KVPair>& kvPairs)
{
    for (auto & [k,v] : kvPairs)
    {
        waitingContents_.push_back(Encode(k,v));
    }
    doWrite();
}

void LogManager::LogWriter::doWrite()
{
    for (const auto & content : waitingContents_)
    {
        ssize_t hawrite = :: write(fd_,content.c_str(),content.size());
        assert(hawrite == content.size());
    }
    ::fsync(fd_);
    waitingContents_.clear();
}

LogManager::LogWriter::~LogWriter()
{
    if(fd_ != -1)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

LogManager::LogWriter::LogWriter(const std::string& name)
 : fileName_(name)
{
    fd_ = ::open(fileName_.c_str(),O_RDWR | O_CLOEXEC | O_CREAT | O_TRUNC,0644);
}

std::unique_ptr<LogManager::LogReader> LogManager::newReader() const
{
    return std::make_unique<LogReader>(fileName_);
}

std::unique_ptr<LogManager::LogWriter> LogManager::newWriter() const
{
    return std::make_unique<LogWriter>(fileName_);
}


std::vector<LogManager::LogReader::KVPair>
LogManager::Decode(char* buf,size_t size)
{
    std::vector<LogReader::KVPair> res;
    size_t current = 0;
    while (current < size)
    {
        uint32_t crc;
        uint32_t keyLength;
        uint32_t valueLength;
        char* begin = buf + current;
        // SafeMemcpy(buf,&crc,current,CheckSumSize,size);
        // SafeMemcpy(buf,&keyLength,current + CheckSumSize,LengthStoreSize,size);
        // SafeMemcpy(buf,&valueLength,current + CheckSumSize + keyLength,LengthStoreSize,size);
        memcpy(&crc,begin,CheckSumSize);
        memcpy(&keyLength,begin + CheckSumSize,LengthStoreSize);
        memcpy(&valueLength,begin + CheckSumSize + LengthStoreSize + keyLength,LengthStoreSize);
        uint32_t expected = CRC::Calculate(begin + CheckSumSize,keyLength + valueLength + 2 * LengthStoreSize,CRC::CRC_32());
        if(expected == crc)
        {
            current += keyLength + valueLength + 2 * LengthStoreSize;
            std::pair<std::string,std::string> kv;
            kv.first = std::string(begin + CheckSumSize + LengthStoreSize,keyLength);
            kv.second = std::string(begin + CheckSumSize + LengthStoreSize * 2 + keyLength,valueLength);
            res.push_back(std::move(kv));
        } else 
        {
            break;
        }
    }
    return res;
}

std::string LogManager::Encode(std::string_view key,std::string_view value)
{
    uint32_t keyLength = key.size();
    uint32_t valueLength = value.size();
    size_t totalSize = key.length() + value.length() + CheckSumSize + 2 * LengthStoreSize;
    std::string buffer(0,totalSize);
    char* begin = const_cast<char*>(buffer.data() + sizeof(uint32_t));    
    char* current = begin;
    memcpy(current,&keyLength,LengthStoreSize);
    current += LengthStoreSize;
    memcpy(current,key.data(),keyLength);
    current += keyLength;

    memcpy(current,&valueLength,LengthStoreSize);
    current += LengthStoreSize;
    memcpy(current,value.data(),valueLength);
    uint32_t crc32 = CRC::Calculate(begin,totalSize - CheckSumSize,CRC::CRC_32());
    memcpy(begin,&crc32,sizeof(uint32_t));
}
