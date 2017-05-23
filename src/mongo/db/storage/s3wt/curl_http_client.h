/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/base/data_builder.h"

namespace mongo {

class CurlHttpClient {
public:
    CurlHttpClient() {}

    StatusWith<std::size_t> read(std::string path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const;
};

}  // namespace mongo
