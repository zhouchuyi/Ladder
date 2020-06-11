#include "format.h"

void AppendUINT32(std::string& buf,uint32_t var)
{
    AppendInteger(buf,var);
}

void AppendUINT64(std::string& buf,uint64_t var)
{
    AppendInteger(buf,var);

}

void AppendSIZET(std::string& buf,size_t var)
{
    AppendInteger(buf,var);
}