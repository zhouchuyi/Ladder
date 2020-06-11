#include <cassert>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "table_builder.h"

void TableBuilder::Add(const std::string_view& key,const std::string_view& value)
{
	entriesNum_++;
	kvBuilder_->Add(key,value);
	lastKey_ = key;
	if(kvBuilder_->CurrentSize() >= blockSize_)
	{
		auto res = WriteBlock();
		kvBuilder_->Reset();
		IndexBuilder_->Add(key,res);
	}
}
