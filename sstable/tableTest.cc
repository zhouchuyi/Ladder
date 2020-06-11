#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <random>
#include <algorithm>

#include "./table_builder.h"
#include "./table.h"

using KVMap = std::map<std::string,std::string>;

static std::string_view RandomString()
{
    static std::string src = std::string("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(src.begin(), src.end(), generator);
    return std::string_view(src.data(),32); 
}

static std::string_view ExtraceUserKey(std::string_view ikey)
{
    assert(ikey.size() >= 8);
    return std::string_view{ikey.data(),ikey.size() - 8};
}

TEST(table,BuildAndLoad)
{
    std::remove("test.table");
    TableBuilder builder("test.table");
    KVMap kvMap;
    for (size_t i = 0; i < 10240; i++)
    {
        std::string randomKey = std::string(RandomString());
        std::string randomValue = std::string(RandomString());
        kvMap.insert(std::make_pair(randomKey,randomKey));
    }
    SequenceNumber seq = 1;
    for (const auto & [k,v] : kvMap)
    {
        InternalKey ikey(k,seq,OpsType::UPDATE);
        builder.Add(ikey.Encode(),std::string_view(v));
        seq++;
    }
    
    builder.Finish();
    ASSERT_EQ(builder.NumEntries(),kvMap.size());
    std::string filename = builder.FileName();
    std::shared_ptr<SSTable> table = SSTable::newTable(filename);
    ASSERT_TRUE(table->isOpen());

    
    int seekCounted_ = 0;
    std::string_view expectedValue;
    auto handle = [&seekCounted_,&expectedValue](const std::string_view& key,const std::string_view& value)
    {
        seekCounted_++;
        ASSERT_EQ(expectedValue,value);
    };

    for (const auto& [k,v] : kvMap)
    {
        InternalKey ikey(k,kDefaultMaxSequenceNumber,OpsType::UPDATE);
        expectedValue = v;
        table->InternalGet(ikey.Encode(),handle);
    }
    
    ASSERT_EQ(seekCounted_,kvMap.size());
    
    {
        std::shared_ptr<SSTable::Iterator> it(table->newIterator());
        for (const auto & [key,value] : kvMap)
        {
            it->Seek(key);
            ASSERT_EQ(ExtraceUserKey(it->key()),key);
            ASSERT_EQ(it->value(),value);
        }
        it->SeekForFirst();
        ASSERT_EQ(ExtraceUserKey(it->key()),kvMap.begin()->first);
        ASSERT_EQ(it->value(),kvMap.begin()->second);
    
        it->SeekForLast();
        ASSERT_EQ(ExtraceUserKey(it->key()),kvMap.rbegin()->first);
        ASSERT_EQ(it->value(),kvMap.rbegin()->second);
        
       for (const auto & [key,value] : kvMap)
       {
           it->Seek(key);
           ASSERT_EQ(ExtraceUserKey(it->key()),key);
           ASSERT_EQ(it->value(),value);
       }

        it->SeekForLast();
        auto rmit = kvMap.rbegin();
        do
        {
            ASSERT_EQ(ExtraceUserKey(it->key()),rmit->first);
            ASSERT_EQ(it->value(),rmit->second);
            rmit++;
            it->Prev();
        } while (it->Valid());

        it->SeekForFirst();
        auto mit = kvMap.begin();
        size_t count = 0;
        do
        {
            ASSERT_EQ(ExtraceUserKey(it->key()),mit->first);
            ASSERT_EQ(it->value(),mit->second);
            mit++;
            it->Next();
            count++;
        } while (it->Valid());
        ASSERT_EQ(count,kvMap.size());

    }
}


