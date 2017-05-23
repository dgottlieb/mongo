/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/data_range.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/stdx/memory.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/mongoutils/str.h"

#include "third_party/wiredtiger/src/include/wiredtiger_ext.h"
#include <wiredtiger.h>

namespace mongo {

class BlockstoreFileHandle;

class File {
public:
    File() : filename(""), fileSize(0) {}
    std::string filename;
    std::int64_t fileSize;
};

class BlockstoreFileSystem : private WT_FILE_SYSTEM {
public:
    BlockstoreFileSystem(std::string dbpath, WT_EXTENSION_API* wtExt)
        : _dbpath(std::move(dbpath)), _wtExt(wtExt) {}

    WT_FILE_SYSTEM* getWtFileSystem() {
        WT_FILE_SYSTEM* ret = static_cast<WT_FILE_SYSTEM*>(this);
        invariant((void*)this == (void*)ret);
        return ret;
    }

    WT_EXTENSION_API* getWtExtension() {
        return _wtExt;
    }

    void addFile(File file) {
        _files[_dbpath + "/" + file.filename] = file;
    }

    bool fileExists(const char* filename) {
        return _files.count(filename) > 0;
    }

    File getFile(const char* filename) {
        auto ret = _files.find(filename);
        uassert(ErrorCodes::NoSuchKey,
                str::stream() << "Filename not found. Filename: " << filename,
                ret != _files.end());

        return ret->second;
    }

    std::int64_t getFileSize(const char* filename) {
        return _files[filename].fileSize;
    }

    int listDirectory(const char* directory,
                      const char* prefix,
                      char*** dirlistp,
                      uint32_t* countp);

    /**
     * `fileHandle` is an out-parameter
     */
    int open(const char* filename, BlockstoreFileHandle** fileHandle);

private:
    stdx::mutex _lock;  // Protects `_files`.
    stdx::unordered_map<std::string, File> _files;

    std::string _dbpath;
    WT_EXTENSION_API* _wtExt; /* Extension functions, e.g outputting errors */
};

class BlockstoreFileHandle : private WT_FILE_HANDLE {
public:
    BlockstoreFileHandle(BlockstoreFileSystem* blockstoreFs) : _blockstoreFs(blockstoreFs) {}

    WT_FILE_HANDLE* getWtFileHandle() {
        WT_FILE_HANDLE* ret = static_cast<WT_FILE_HANDLE*>(this);
        invariant((void*)this == (void*)ret);
        return ret;
    }

    /**
     * return 0 on success, non-zero on error. As per WT expectations.
     */
    int read(void* buf, std::size_t offset, std::size_t length);

    int write(const void* buf, std::size_t offset, std::size_t length);

    int sync();

private:
    BlockstoreFileSystem* _blockstoreFs;

    stdx::mutex _lock;  // Protects `_fileSize` and `_contents`.
    long long _fileSize;
};

}  // namespace mongo
