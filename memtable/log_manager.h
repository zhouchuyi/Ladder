#pragma once

#include <string>
#include <memory>
#include <string_view>
#include <vector>


class LogManager
{
private:
    static std::string kDefaultLogFileName;
    std::string fileName_;

    LogManager(const std::string& LogFileName = kDefaultLogFileName)
     : fileName_(LogFileName)
    {

    }

    ~LogManager() = default;
public:
   
    static LogManager& instance(std::string LogName = kDefaultLogFileName)
    {
        static LogManager manager_(LogName);
        return manager_;
    }


    struct LogReader
    {
    private:
        int fd_{-1};
        size_t fileSize_{0};
        void check(std::string_view slice,uint32_t checksum);
        std::string fileName_;
    public:
        LogReader(const std::string& name)
         : fileName_(name)
        {

        }
        ~LogReader();
        using KVPair = std::pair<std::string,std::string>;
        size_t fileSize() const { return fileSize_; };
        int Open();
        std::vector<KVPair> LoadToEnd();
    private:
        std::vector<KVPair> doRead(bool isToEnd);
    };

    struct LogWriter
    {
    private:

        int fd_;
        std::string fileName_;        
        void doWrite();
        std::vector<std::string> waitingContents_;
    public:

        LogWriter(const std::string& name);
        
        ~LogWriter();

        using KVPair = std::pair<std::string_view,std::string_view>;
        
        //write 
        void Add(std::string_view key,std::string_view value);

        void Add(const std::vector<KVPair>& kvPairs);
    
    };

    std::unique_ptr<LogReader> newReader() const;

    std::unique_ptr<LogWriter> newWriter() const;

private:

    static std::vector<LogReader::KVPair> Decode(char* buf,size_t size);
    static uint32_t calCRC32(std::string_view slice);
    static std::string Encode(std::string_view key,std::string_view value);


};

