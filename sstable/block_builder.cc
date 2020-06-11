#include "block_builder.h"

template<>
void KVBlockBuilder::Add(const std::string_view& key,const std::string_view& value)
{
    if(size_++ % interval_ == 0)
    {
        restarts_.push_back(buffer_.size());
    }

    AppendUINT32(buffer_,key.length());
    buffer_.append(key);

    AppendUINT32(buffer_,value.length());
    buffer_.append(value);
    
    lastkey_ = key;
}

template<>
void IndexBlockBuilder::Add(const std::string_view& key,const std::pair<size_t,size_t>& value)
{

    restarts_.push_back(buffer_.size());


    AppendUINT32(buffer_,key.length());
    size_t size = buffer_.size();
    buffer_.append(key);
    // assert(key == std::string_view(buffer_.data() + size,32));
    AppendSIZET(buffer_,value.first);
    AppendSIZET(buffer_,value.second);
    
    lastkey_ = key;
}