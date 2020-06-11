#include <string>
#include <cstring>
#include <type_traits>
#include <cassert>
void AppendUINT32(std::string& buf,uint32_t var);

void AppendUINT64(std::string& buf,uint64_t var);

void AppendSIZET(std::string& buf,size_t var);

template<typename T>
void AppendInteger(std::string& buf,T var)
{
    static_assert(std::is_integral<T>::value,"append val should be interger");
    size_t newSize = buf.size() + sizeof(var);
    size_t oldSize = buf.size();
    buf.reserve(newSize * 1.5);
    buf.resize(newSize);  
    char* begin = const_cast<char*>(buf.data() + oldSize); 
    memcpy(begin,&var,sizeof(T));
  
}


