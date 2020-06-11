#pragma once

#include <string_view>

#include "InternalKey.h"

struct StringViewComparator
{
    int operator()(const std::string_view& l,const std::string_view& r) const
    {
        if(l == r)
            return 0;
        else if (l < r)
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }
};


struct StringComparator
{
    int operator()(const std::string& l,const std::string& r) const
    {
        if(l == r)
            return 0;
        else if (l < r)
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }
};


struct InternalKeyComparator
{
    int operator()(const InternalKey& l,const InternalKey& r) const
    {
        std::string_view userKeyl = l.ExtractUserKey();
        std::string_view userKeyr = r.ExtractUserKey();
        int res = StringViewComparator{}(userKeyl,userKeyr);
        if(res == 0)
        {
            uint64_t lnum = l.ExtractInteger();
            uint64_t rnum = r.ExtractInteger();
            if(lnum < rnum)
            {
                res = -1;
            } else if (lnum > rnum)
            {
                res = 1;
            }
        }
        return res;
    }
};

struct InternalKeyStringViewComparator
{
    int operator()(const std::string_view& l,const std::string_view& r) const
    {
        InternalKey lkey;
        lkey.DecodeFrom(l);
        InternalKey rkey;
        rkey.DecodeFrom(r);
        return InternalKeyComparator{}(lkey,rkey);
    }
   
};


struct InternalKeyUserComparator
{
    int operator()(const std::string_view& l,const std::string_view& r) const
    {
        assert(l.size() >= 8 && r.size() >= 8);
        std::string_view userKeyl = std::string_view(l.data(),l.size() - 8);
        std::string_view userKeyr = std::string_view(r.data(),r.size() - 8);
        return StringViewComparator{}(userKeyl,userKeyr);
    }
};
