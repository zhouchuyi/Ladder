#include "skiplist.h"
#include "../util/Compare.h"
#include <gtest/gtest.h>
#include <set>
#include <map>
#include <random>

static std::string_view RandomString()
{
    static std::string src = std::string("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(src.begin(), src.end(), generator);
    return std::string_view(src.data(),32); 
}

auto compare = [](const int& l,const int& r)
                {
                    if(l == r)
                        return 0;
                    else if (l < r)
                    {
                        return 1;
                    } else
                    {
                        return -1;
                    }                      
                };

using IntSlist = SkipList<int,int,decltype(compare)>;

TEST(SkipList,Empty)
{
    IntSlist list(compare);
    auto res = list.find(100);
    ASSERT_EQ(res.first,false);

    auto bi = list.begin();
    auto end = list.end();
    ASSERT_TRUE(!bi.Valid());
    ASSERT_EQ(bi,end);
}

TEST(SkipList,InsertAndLookup)
{
    constexpr int N = 2000;
    constexpr int R = 5000;
    std::set<int> keys;
    IntSlist list(compare);
    for (size_t i = 0; i < N; i++)
    {
        int key = std::rand() % R;
        if(keys.insert(key).second)
            list.insert(int(key),0);
    }
    for (size_t i = 0; i < R; i++)
    {
        if(list.find(i).first)
            ASSERT_EQ(keys.count(i),1);
        else
            ASSERT_EQ(keys.count(i),0);
            
    }
    
    {
        auto bi = list.begin();
        std::cout << bi.key() << std::endl;
        auto it = keys.begin();
        while (bi != list.end())
        {
            ASSERT_EQ(bi.key(),*it);
            bi.Next();
            it++;
        }
    }

}

TEST(SkipList,StringKVTest)
{
    std::map<std::string,std::string> kvMap;
    SkipList<std::string,std::string,StringComparator> kvSkipList(StringComparator{});

    for (size_t i = 0; i < 1024; i++)
    {
        std::string_view randomKey = RandomString();
        kvMap.insert(std::make_pair(std::string(randomKey),std::string(randomKey)));
    }
    
    for (const auto & [k ,v] : kvMap)
    {
        kvSkipList.insert(k,v);
    }
    
    {
        auto it = kvSkipList.begin();
        auto mit = kvMap.begin();
        size_t count = 0;
        while (it.Valid())
        {
            ASSERT_EQ(it.key(),mit->first);
            ASSERT_EQ(it.value(),mit->second);
            count++;
            it.Next();
            mit++;
        }
        ASSERT_EQ(count,kvMap.size());
    }

    {
        auto it = kvSkipList.begin();
        it.SeekForLast();
        auto mit = kvMap.rbegin();
        size_t count = 0;
        while (it.Valid())
        {
            ASSERT_EQ(it.key(),mit->first);
            ASSERT_EQ(it.value(),mit->second);
            count++;
            it.Prev();
            mit++;
        }
        ASSERT_EQ(count,kvMap.size());
    }

    {
        auto it = kvSkipList.begin();
        for (const auto & [k ,v] : kvMap)
        {
            it.Seek(k);
            ASSERT_EQ(it.key(),k);
            ASSERT_EQ(it.value(),v);
        }
        
    }
}   