#include <gtest/gtest.h>
#include <set>
#include <map>
#include <random>
#include <iostream>
#include "memtable.h"

static std::string_view RandomString()
{
    static std::string src = std::string("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(src.begin(), src.end(), generator);
    return std::string_view(src.data(),32); 
}

using kvMap = std::map<std::string,std::string>;
TEST(MemTable,SetAndGet)
{
    {
        MemTable table;
        kvMap map;
        SequenceNumber seq = 1;
        for (size_t i = 0; i < 64; i++)
        {
            std::string randomKey(RandomString());
            map[randomKey] = randomKey;
        }
        for (auto & pair : map)
        {
            table.Add(pair.first,pair.second,seq++);
        }
        for (auto & pair : map)
        {
            std::string value;
            table.Get(pair.first,value);
            ASSERT_EQ(value,pair.second);
        }
        {
            auto it = table.newIterator();
            it->SeekForFirst();
            auto mit = map.begin();
            do
            {
                ASSERT_EQ(it->key().ExtractUserKey(),mit->first);
                ASSERT_EQ(it->value(),mit->second);
                it->Next();
                mit++;
            } while (it->Valid());
            
        }
        {
            auto it = table.newIterator();
            it->SeekForLast();
            auto mit = map.rbegin();
            do
            {
                ASSERT_EQ(it->key().ExtractUserKey(),mit->first);
                ASSERT_EQ(it->value(),mit->second);
                it->Prev();
                mit++;
            } while (it->Valid());   
        }
    }
}
// 1 > 2 > 
TEST(MemTable,SequenceTest)
{
    MemTable table;
    SequenceNumber seq = 1;
    std::string randomKey(RandomString());
    size_t i = 0;
    for (; i < 64; i++)
    {
        table.Add(randomKey,std::to_string(i),seq++);
    }
    {
        auto it = table.newIterator();
        it->SeekForFirst();
        size_t cnt = 0;
        size_t v = 63;
        do
        {
            ASSERT_EQ(it->value(),std::to_string(v));
            it->Next();
            cnt++;
            v--;
        } while (it->Valid());   
        delete it;
        ASSERT_EQ(cnt,64);
    }
    std::string value;
    table.Get(randomKey,value);
    ASSERT_EQ(value,std::to_string(i - 1));
}