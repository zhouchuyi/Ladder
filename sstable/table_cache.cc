#include "table_cache.h"
#include "../util/fname.h"

static void tableDeleter(const std::string &key, void *value)
{
    SSTable* table = static_cast<SSTable*>(value);
    delete table;
}

Entry* TableCache::findTable(uint64_t fileNumber,uint64_t fileSize)
{
    char buf[sizeof(fileNumber)];
    memcpy(buf,&fileNumber,sizeof(fileNumber));

    std::string key(buf);

    Entry* entry = cache_->Lookup(key);
    if(entry == nullptr)
    {
        std::string fname = TableFileName(dbname_,fileNumber);
        SSTable* table = SSTable::newTableRaw(fname);
        entry = cache_->Insert(key,table,1,tableDeleter);
    }
    return entry;
}