#include "block.h"
#include "block_builder.h"
#include "../util/InternalKey.h"
#include <gtest/gtest.h>


#include <map>
#include <vector>
#include <random>
static std::string_view RandomString()
{
    static std::string src = std::string("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(src.begin(), src.end(), generator);
    return std::string_view(src.data(),32); 
}

// static std::string_view LookUpKey(const std::string_view userKey)
// {
//     InternalKey key(userKey,kDefaultMaxSequenceNumber,OpsType::UPDATE);
//     return key.Encode();
// }

static std::string_view ExtraceUserKey(std::string_view ikey)
{
    assert(ikey.size() >= 8);
    return std::string_view{ikey.data(),ikey.size() - 8};
}

static std::string_view readKey(const char* data)
{
    char* begin = const_cast<char*>(data + 4);
    return std::string_view(begin,32);
}

TEST(BLOCKBUILDER,indexBuilder)
{
    IndexBlockBuilder builder;
    std::map<std::string,std::pair<size_t,size_t>> locationMap;
    std::vector<std::string> keys;
    size_t totalSize = 0;
    for (size_t i = 0; i < 1000; i++)
    {
        std::string_view randomKey = RandomString();
        std::pair<size_t,size_t> location{i,1}; //offset and block size
        auto res = locationMap.insert(std::make_pair(std::string(randomKey),location));
        if(res.second)
        {
            // builder.Add(randomKey,location);
            // keys.push_back(std::string(randomKey));
        }
        totalSize += sizeof(uint32_t) + randomKey.size() + sizeof(size_t) * 2;
    }
    totalSize += (locationMap.size() + 1) * sizeof(uint32_t);
    SequenceNumber seq = 1;
    for (const auto & [key,value] : locationMap)
    {
        InternalKey ikey(key,seq,OpsType::UPDATE);
        keys.push_back(key);
        builder.Add(ikey.Encode(),value);
        seq++;
    }

    std::string_view content = builder.Finish();
    BlockContent blockContent{content.data(),content.size(),false};
    IndexBlock indexBlock_(std::move(blockContent),InternalKeyStringViewComparator{});
    ASSERT_EQ(indexBlock_.NumRestarts(),locationMap.size());

    
    std::shared_ptr<IndexIterator> it = std::shared_ptr<IndexIterator>(indexBlock_.newIterator(),[](IndexIterator* it){
        delete it;
    });

    {
        it->SeekForLast();
        ASSERT_EQ(ExtraceUserKey(it->key()),keys.back());
        ASSERT_EQ(it->value(),locationMap[keys.back()]);
    }

    {
        it->SeekForFirst();
        ASSERT_EQ(ExtraceUserKey(it->key()),keys[0]);
        ASSERT_EQ(it->value(),locationMap[keys[0]]);
    }
    
    for (auto & key : keys)
    {
        if(key == keys[0])
            continue;
        std::string_view keyVew(key);
        it->Next();
        ASSERT_EQ(keyVew,ExtraceUserKey(it->key()));
        ASSERT_EQ(it->value(),locationMap[std::string(keyVew)]);
    }
        
    for (const auto & [key,value] : locationMap)
    {
        std::string_view keyVew = key;
        InternalKey ikey(keyVew,kDefaultMaxSequenceNumber,OpsType::UPDATE);
        it->Seek(ikey.Encode());
        ASSERT_EQ(keyVew,ExtraceUserKey(it->key()));
        ASSERT_EQ(value,it->value());
    } 
    

    {
        it->SeekForLast();
        auto mit = locationMap.rbegin();
        do
        {
            ASSERT_EQ(ExtraceUserKey(it->key()),mit->first);
            ASSERT_EQ(it->value(),mit->second);
            mit++;
            it->Prev();
        } while (it->Valid());
        
    }

    {
        it->SeekForFirst();
        auto mit = locationMap.begin();
        size_t count = 0;
        do
        {
            ASSERT_EQ(ExtraceUserKey(it->key()),mit->first);
            ASSERT_EQ(it->value(),mit->second);
            mit++;
            it->Next();
            count++;
        } while (it->Valid());
        ASSERT_EQ(count,locationMap.size());  
    }
}



TEST(BLOCKBUILDER,KVBuilder)
{
    KVBlockBuilder builder;
    std::map<std::string,std::string> kvMap;
    std::vector<std::string> keys;
    size_t totalSize = 0;
    for (size_t i = 0; i < 1600; i++)
    {
        std::string_view randomKey = RandomString();
        auto res = kvMap.insert(std::make_pair(std::string(randomKey),std::string(randomKey)));
    }
    SequenceNumber seq = 1;
    for (const auto & [k,v] : kvMap)
    {
        InternalKey ikey(k,seq,OpsType::UPDATE);
        builder.Add(ikey.Encode(),v);
        keys.push_back(k);
        seq++;
    }
    std::string_view content = builder.Finish();
    const char* data = content.data();
    BlockContent blockContent{content.data(),content.size(),false};
    KVBlock kvBlock_(std::move(blockContent),InternalKeyStringViewComparator{});
    

    std::shared_ptr<KVIterator> it = std::shared_ptr<KVIterator>(kvBlock_.newIterator());

    for (const auto & [key,value] : kvMap)
    {
        std::string_view keyVew = key;
        InternalKey ikey(keyVew,kDefaultMaxSequenceNumber,OpsType::UPDATE);
        it->Seek(ikey.Encode());
        ASSERT_EQ(keyVew,ExtraceUserKey(it->key()));
        ASSERT_EQ(value,it->value());
    } 

    {
        it->SeekForLast();
        ASSERT_EQ(ExtraceUserKey(it->key()),keys.back());
        ASSERT_EQ(it->value(),kvMap[keys.back()]);
    }

    {
        it->SeekForFirst();
        ASSERT_EQ(ExtraceUserKey(it->key()),keys[0]);
        ASSERT_EQ(it->value(),kvMap[keys[0]]);
    }

    for (auto & key : keys)
    {
        if(key == keys[0])
            continue;
        std::string_view keyVew(key);
        it->Next();
        ASSERT_EQ(keyVew,ExtraceUserKey(it->key()));
        ASSERT_EQ(it->value(),kvMap[std::string(keyVew)]);
    }



    {
        it->SeekForLast();
        auto mit = kvMap.rbegin();
        size_t count = 0;
        do
        {
            ASSERT_EQ(ExtraceUserKey(it->key()),mit->first);
            ASSERT_EQ(it->value(),mit->second);
            mit++;
            it->Prev();
            count++;
        } while (it->Valid());
        ASSERT_EQ(count,kvMap.size());   
    }

    {
        it->SeekForFirst();
        auto mit = kvMap.begin();
        size_t count = 0;
        do
        {
            count++;
            ASSERT_EQ(ExtraceUserKey(it->key()),mit->first);
            ASSERT_EQ(it->value(),mit->second);
            mit++;
            it->Next();
        } while (it->Valid());
        ASSERT_EQ(count,kvMap.size());        
    }

}






