/**
*  Copyright (C) 2016 MongoDB Inc.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "blockstore_fs.h"

#include <string.h>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/allocator.h"
#include "mongo/util/log.h"

#include "curl_http_client.h"

using mongo::operator""_sd;

extern "C" {

static int s3WtFsDirectoryList(WT_FILE_SYSTEM* file_system,
                               WT_SESSION* session,
                               const char* directory,
                               const char* prefix,
                               char*** dirlistp,
                               uint32_t* countp) {
    std::cout << "List." << std::endl;
    std::cout << "List. Dir: " << (directory == nullptr ? "null" : directory)
              << " Prefix: " << (prefix == nullptr ? "null" : prefix) << std::endl;

    auto blockstoreFs = reinterpret_cast<mongo::BlockstoreFileSystem*>(file_system);
    return blockstoreFs->listDirectory(directory, prefix, dirlistp, countp);
}

static int s3WtFsDirectoryListFree(WT_FILE_SYSTEM* file_system,
                                   WT_SESSION* session,
                                   char** dirlist,
                                   uint32_t count) {
    if (dirlist != NULL) {
        while (count > 0) {
            free(dirlist[--count]);
        }
        free(dirlist);
    }
    return 0;
}

/*
 * Forward function declarations for file system API implementation
 */
static int s3WtFsFileOpen(
    WT_FILE_SYSTEM*, WT_SESSION*, const char*, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE**);
static int s3WtFsFileExist(WT_FILE_SYSTEM*, WT_SESSION*, const char*, bool*);
static int s3WtFsFileSize(WT_FILE_SYSTEM*, WT_SESSION*, const char*, wt_off_t*);
static int s3WtFsRemove(WT_FILE_SYSTEM*, WT_SESSION*, const char*, uint32_t);
static int s3WtFsRename(WT_FILE_SYSTEM*, WT_SESSION*, const char*, const char*, uint32_t);
static int s3WtFsTerminate(WT_FILE_SYSTEM*, WT_SESSION*);

/*
 * Forward function declarations for file handle API implementation
 */
static int s3WtFileRead(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t, size_t, void*);
static int s3WtFileSize(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t*);
static int s3WtFileLock(WT_FILE_HANDLE*, WT_SESSION*, bool);
static int s3WtFileClose(WT_FILE_HANDLE*, WT_SESSION*);
static int s3WtFileSize(WT_FILE_HANDLE*, WT_SESSION*);
static int s3WtFileWrite(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t, size_t, const void*);

/*
 * s3WtFsCreate --
 *   Initialization point for the s3 file system
 */
int s3WtFsCreate(WT_CONNECTION* conn, WT_CONFIG_ARG* config) {
    std::cout << "initing." << std::endl;

    WT_CONFIG_ITEM k, v;
    WT_CONFIG_PARSER* config_parser;
    WT_EXTENSION_API* wtext;
    int ret = 0;

    std::string dbpath;

    wtext = conn->get_extension_api(conn);

    // Open a WiredTiger parser on the "config" value.
    if ((ret = wtext->config_parser_open_arg(wtext, nullptr, config, &config_parser)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_EXTENSION_API.config_parser_open: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // Step through our configuration values.
    while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
        if (mongo::StringData(k.str, k.len) == "dbpath"_sd) {
            dbpath = std::string(v.str, v.len);
            continue;
        }

        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.next: unexpected configuration "
                                "information: %.*s=%.*s: %s",
                                (int)k.len,
                                k.str,
                                (int)v.len,
                                v.str,
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // Check for expected parser termination and close the parser.
    if (ret != WT_NOTFOUND) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.next: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    if ((ret = config_parser->close(config_parser)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.close: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // mongo::StatusWith<std::vector<mongo::File>> swFiles(mongo::ErrorCodes::BadValue, "init");
    // if (!swFiles.isOK()) {
    //     (void)wtext->err_printf(
    //         wtext, nullptr, "ListDir: %s", swFiles.getStatus().reason().c_str());
    //     exit(1);
    // }

    auto blockstoreFs =
        mongo::stdx::make_unique<mongo::BlockstoreFileSystem>(std::move(dbpath), wtext);

    // for (const auto& file : swFiles.getValue()) {
    //     blockstoreFs->addFile(file);
    // }

    WT_FILE_SYSTEM* wtFileSystem = blockstoreFs->getWtFileSystem();
    memset(wtFileSystem, 0, sizeof(WT_FILE_SYSTEM));

    // Initialize the filesystem interface jump table.
    wtFileSystem->fs_open_file = s3WtFsFileOpen;
    wtFileSystem->fs_exist = s3WtFsFileExist;
    wtFileSystem->fs_size = s3WtFsFileSize;
    wtFileSystem->fs_remove = s3WtFsRemove;
    wtFileSystem->fs_rename = s3WtFsRename;
    wtFileSystem->terminate = s3WtFsTerminate;

    wtFileSystem->fs_directory_list = s3WtFsDirectoryList;
    wtFileSystem->fs_directory_list_free = s3WtFsDirectoryListFree;

    if ((ret = conn->set_file_system(conn, wtFileSystem, nullptr)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONNECTION.set_file_system: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // WT will call a filesystem terminate method and pass in the pointer `blockstoreFs` for
    // cleaning up.
    blockstoreFs.release();
    std::cout << "Returning." << std::endl;
    return 0;
}

static int s3WtFsFileOpen(WT_FILE_SYSTEM* file_system,
                          WT_SESSION* session,
                          const char* name,
                          WT_FS_OPEN_FILE_TYPE file_type,
                          uint32_t flags,
                          WT_FILE_HANDLE** file_handlep) {
    std::cout << "Open. Name: " << name << std::endl;
    auto blockstoreFs = reinterpret_cast<mongo::BlockstoreFileSystem*>(file_system);

    return blockstoreFs->open(name, reinterpret_cast<mongo::BlockstoreFileHandle**>(file_handlep));
}

static int s3WtFsFileExist(WT_FILE_SYSTEM* file_system,
                           WT_SESSION* session,
                           const char* name,
                           bool* existp) {
    std::cout << "Exist. Name: " << name << std::endl;
    auto blockstoreFs = reinterpret_cast<mongo::BlockstoreFileSystem*>(file_system);
    *existp = blockstoreFs->fileExists(name);

    return 0;
}

static int s3WtFsFileSize(WT_FILE_SYSTEM* file_system,
                          WT_SESSION* session,
                          const char* name,
                          wt_off_t* sizep) {
    std::cout << "Size. Name: " << name << std::endl;
    auto blockstoreFs = reinterpret_cast<mongo::BlockstoreFileSystem*>(file_system);
    if (!blockstoreFs->fileExists(name)) {
        return ENOENT;
    }

    // *sizep = blockstoreFs->getFileSize(name);
    return 0;
}

static int s3WtFsRemove(WT_FILE_SYSTEM* file_system,
                        WT_SESSION* session,
                        const char* name,
                        uint32_t flags) {
    std::cout << "Remove. Name: " << name << std::endl;
    return 0;
}

static int s3WtFsRename(WT_FILE_SYSTEM* file_system,
                        WT_SESSION* session,
                        const char* from,
                        const char* to,
                        uint32_t flags) {
    std::cout << "Rename. From: " << from << " To: " << to << std::endl;
    return 0;
}

static int s3WtFsTerminate(WT_FILE_SYSTEM* file_system, WT_SESSION* session) {
    auto blockstoreFs = reinterpret_cast<mongo::BlockstoreFileSystem*>(file_system);
    delete blockstoreFs;

    return 0;
}

static int s3WtFileRead(
    WT_FILE_HANDLE* file_handle, WT_SESSION* session, wt_off_t offset, size_t len, void* buf) {
    auto fileHandle = reinterpret_cast<mongo::BlockstoreFileHandle*>(file_handle);

    return fileHandle->read(buf, offset, len);
}

static int s3WtFileWrite(WT_FILE_HANDLE* file_handle,
                         WT_SESSION* session,
                         wt_off_t offset,
                         size_t len,
                         const void* buf) {
    auto fileHandle = reinterpret_cast<mongo::BlockstoreFileHandle*>(file_handle);

    return fileHandle->write(buf, offset, len);
}

static int s3WtFileSync(WT_FILE_HANDLE* file_handle, WT_SESSION* session) {
    auto fileHandle = reinterpret_cast<mongo::BlockstoreFileHandle*>(file_handle);

    return fileHandle->sync();
}

static int s3WtFileSize(WT_FILE_HANDLE* file_handle, WT_SESSION* session, wt_off_t* sizep) {
    // auto fileHandle = reinterpret_cast<mongo::BlockstoreFileHandle*>(file_handle);

    // *sizep = (wt_off_t)fileHandle->getFileSize();
    return 0;
}

static int s3WtFileLock(WT_FILE_HANDLE* file_handle, WT_SESSION* session, bool lock) {
    // Locks are always granted.
    return 0;
}

static int s3WtFileClose(WT_FILE_HANDLE* baseFileHandle, WT_SESSION* session) {
    free(baseFileHandle->name);

    auto blockstoreFileHandle = reinterpret_cast<mongo::BlockstoreFileHandle*>(baseFileHandle);
    delete blockstoreFileHandle;

    return 0;
}
}

namespace mongo {
namespace {
bool endsWith(const std::string& value, const std::string& ending) {
    if (ending.size() > value.size())
        return false;

    return value.compare(value.length() - ending.size(), ending.size(), ending) == 0;
}
}  // namespace

int BlockstoreFileSystem::open(const char* name, BlockstoreFileHandle** fileHandle) {
    std::string filename(name);
    if (endsWith(filename, "WiredTiger.lock")) {
        return ENOENT;
    }

    if (!fileExists(name)) {
        return ENOENT;
    }

    auto file = getFile(name);

    auto ret = stdx::make_unique<BlockstoreFileHandle>(this);
    if (ret == nullptr) {
        return ENOMEM;
    }

    /* Initialize public information. */
    WT_FILE_HANDLE* baseFileHandle = ret->getWtFileHandle();
    memset(baseFileHandle, 0, sizeof(WT_FILE_HANDLE));
    if ((baseFileHandle->name = strdup(name)) == nullptr) {
        return ENOMEM;
    }

    /*
     * Setup the function call table for our custom file system. Set the function pointer to
     * nullptr
     * where our implementation doesn't support the functionality.
     */
    baseFileHandle->close = s3WtFileClose;
    baseFileHandle->fh_read = s3WtFileRead;
    baseFileHandle->fh_size = s3WtFileSize;
    baseFileHandle->fh_lock = s3WtFileLock;

    baseFileHandle->fh_sync = s3WtFileSync;
    baseFileHandle->fh_write = s3WtFileWrite;

    *fileHandle = ret.release();
    return 0;
}

int BlockstoreFileSystem::listDirectory(const char* directory,
                                        const char* prefix,
                                        char*** dirlistp,
                                        uint32_t* countp) {
    char** entries = nullptr;
    int allocated = 0;
    int count = 0;
    stdx::lock_guard<stdx::mutex> lock(_lock);
    for (auto it = _files.begin(); it != _files.end(); it++) {
        if (count >= allocated) {
            entries = (char**)realloc(entries, (allocated + 10) * sizeof(char*));
            memset(entries + allocated * sizeof(char*), 0, 10 * sizeof(char*));
            allocated += 10;
        }
        entries[count++] = strdup(it->second.filename.c_str());
    }

    *dirlistp = entries;
    *countp = count;
    return 0;
}

int BlockstoreFileHandle::read(void* buf, std::size_t offset, std::size_t length) {
    if (offset > static_cast<std::size_t>(_fileSize)) {
        return EINVAL;
    }

    if (length > _fileSize - offset) {
        length = _fileSize - offset;
    }

    /*
        int ret;
        std::string msg;
        mongo::DataRange wrappedBuf(reinterpret_cast<char*>(buf), length);
        auto swBytesRead = _reader->read(wrappedBuf, offset, length);
        if (!swBytesRead.isOK()) {
            ret = EIO;
            log() << swBytesRead.getStatus().reason();
        } else if (swBytesRead.getValue() != length) {
            ret = EAGAIN;
            log() << "Read < Length. Read: " << swBytesRead.getValue() << "Length: " << length;
        } else {
            ret = 0;
        }

        return ret;
    */

    return 0;
}

int BlockstoreFileHandle::write(const void* buf, std::size_t offset, std::size_t length) {
    return 0;
}

int BlockstoreFileHandle::sync() {
    return 0;
}
}  // namespace mongo
