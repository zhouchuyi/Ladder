#include <gtest/gtest.h>
#include <map>
#include <set>
#include <vector>
#include <random>
#include <algorithm>

#include "./table_builder.h"
#include "./table.h"
#include "./merge.h"

using KVMap = std::map<std::string,std::string>;
static std::string_view ExtraceUserKey(std::string_view ikey)
{
    assert(ikey.size() >= 8);
    return std::string_view{ikey.data(),ikey.size() - 8};
}
static std::string_view RandomString()
{
    static std::string src = std::string("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(src.begin(), src.end(), generator);
    return std::string_view(src.data(),32); 
}

TEST(MergeIterator,Merge)
{
    std::vector<std::string> tablesNames{"MergeTest.tabe1","MergeTest.tabe2"};
    std::vector<KVMap> kvmaps;
    std::vector<std::shared_ptr<SSTable>> tableList;
    SequenceNumber seq = 1;
    size_t totalSize = 0;
    std::set<std::string> keys;
    for (size_t i = 0; i < 1024; i++)
    {
        std::string randomKey = std::string(RandomString());
        keys.insert(std::string(randomKey));
    }
    for (auto & fname : tablesNames)
    {
        std::remove(fname.c_str());
        TableBuilder builder(fname.c_str());
        KVMap kvMap;
        for (const auto & k : keys)
        {
            InternalKey key(k,seq,OpsType::UPDATE);
            builder.Add(key.Encode(),std::string_view(fname));
            kvMap[k] = fname;
            seq++;
            totalSize++;
        }
        builder.Finish();
        ASSERT_EQ(builder.NumEntries(),kvMap.size());
        std::string filename = builder.FileName();
        std::shared_ptr<SSTable> table = SSTable::newTable(filename);
        ASSERT_TRUE(table->isOpen());
        kvmaps.push_back(kvMap);
        tableList.push_back(table);
    }
    ASSERT_EQ(tableList.size(),tablesNames.size());
    {
        MergeIterator::IteratorList itList;
        for (auto & table : tableList)
        {
            itList.emplace_back(table->newIterator());
        }
        auto it = std::make_shared<MergeIterator>(std::move(itList));
        it->SeekForFirst();
        
        std::vector<KVMap::iterator> kvmapIterators;
        for (auto & map : kvmaps)
        {
            kvmapIterators.push_back(map.begin());
        }

        int cnt = 0;
        do
        {
            // std::cout << it->key() <<" "<< it->value() << std::endl;
            KVMap::iterator& mapit = kvmapIterators[tableList.size() - cnt % tableList.size() - 1];
            ASSERT_EQ(mapit->first,ExtraceUserKey(it->key()));
            ASSERT_EQ(mapit->second,it->value());
            it->Next();
            cnt++;
            mapit++;
        } while (it->Valid());
        ASSERT_EQ(cnt,totalSize);
        // ASSERT_EQ(kvmapIterators[0],kvmaps[0].end());        
        // ASSERT_EQ(kvmapIterators[1]，kvmaps[1].end());        

    }    
    



}