#pragma once

#include <string_view>
#include <string>
#include <cassert>
#include <cstring>
#include <limits>
using SequenceNumber = uint64_t;

enum class OpsType {
    DELETE = 0x0,
    UPDATE = 0x1,
};
static constexpr SequenceNumber kDefaultMaxSequenceNumber = std::numeric_limits<SequenceNumber>::max();


class InternalKey
{
private:
    std::string key_;    
public:
    InternalKey() = default;
    InternalKey(const std::string_view& userKey,SequenceNumber s,OpsType type)
    {
        ParsedInternalKey pkey{userKey,s,type};
        key_ = pkey.SerializeInternalKey();
    }
    ~InternalKey() = default;
    bool operator == (const InternalKey& k) const
    {
        return key_ == k.key_;
    }
    struct ParsedInternalKey
    {
        std::string_view userKey_;
        SequenceNumber seq_;
        OpsType type_;
        
        ParsedInternalKey() = default;

        ParsedInternalKey(const std::string_view& u,SequenceNumber number,OpsType type)
         : userKey_(u),
           seq_(number),
           type_(type)
        {

        }
        size_t EncodingLength() const 
        {
            return userKey_.length() + 8;
        }

        std::string SerializeInternalKey()
        {
            std::string key(userKey_.size() + 8,0);
            char* begin = const_cast<char*>(key.data());
            memcpy(begin,userKey_.data(),userKey_.size());
            begin += userKey_.size();
            uint64_t packSeqAndType = (seq_ << 8) | uint8_t(type_);
            memcpy(begin,&packSeqAndType,sizeof(packSeqAndType));
            return key;
        }

    };
    
    std::string_view ExtractUserKey() const
    {
        assert(key_.size() >= 8);
        return std::string_view(key_.data(),key_.size() - 8);
    }

    uint64_t ExtractInteger() const
    {
        uint64_t res;
        memcpy(&res,key_.data() + key_.size() - 8,sizeof(uint64_t));
        return res;
    }

    std::string_view Encode() const
    {
        assert(!key_.empty());
        return key_;
    }

    bool DecodeFrom(const std::string_view& s)
    {
        key_.assign(s.begin(),s.end());
        return !s.empty();
    }

    bool ParseInternalKey(ParsedInternalKey& res) const
    {
        if(key_.size() < 8)
            return false;

        std::string_view userKey{key_.data(),key_.size() - 8};
        uint64_t packSeqAndType;
        SequenceNumber seq;
        uint8_t type;
        memcpy(&packSeqAndType,key_.data() + key_.size() - 8,sizeof(packSeqAndType));
        type = packSeqAndType & 0xff;
        seq = packSeqAndType >> 8;

        res.userKey_ = userKey;
        res.seq_ = seq;
        res.type_ = static_cast<OpsType>(type);

        return type <= static_cast<uint8_t>(OpsType::UPDATE);
    }

    

};


