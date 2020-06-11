#pragma once

#include <set>
#include <vector>
#include <utility>

#include "../util/InternalKey.h"

struct FileMeta
{
    uint64_t number_{0};
    uint64_t fileSize_{0};
    InternalKey largest_;
    InternalKey smallest_;
};


class VersionEdit
{
private:
    using DeletedFileSet = std::set<std::pair<int,uint64_t>>;
    SequenceNumber lastSequence_;
    std::vector<std::pair<int,FileMeta>> newFiles;
public:
    VersionEdit();
    ~VersionEdit();

    void AddFile(int level,uint64_t file,uint64_t fileSize,
                    const InternalKey& smallest,const InternalKey& largest);


    void RemoveFile();
};



class Version
{
private:

public:
    Version();
    ~Version();

    FileMeta* Get(const InternalKey& key,std::string& val);

    

};



