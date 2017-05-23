/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "curl_http_client.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include <iterator>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/exit.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace queryable {

class CurlLibraryManager {
public:
    ~CurlLibraryManager() {
        if (_initialized.load()) {
            curl_global_cleanup();
        }
    }

    // initializes curl, idempotent
    void initialize() {
        if (_initialized.compareAndSwap(false, true) == false) {
            curl_global_init(CURL_GLOBAL_ALL & ~CURL_GLOBAL_SSL);
        }
    }

private:
    AtomicWord<bool> _initialized;
};

namespace {
size_t WriteMemoryCallback(void* ptr, size_t size, size_t nmemb, void* data) {
    size_t realsize = size * nmemb;

    DataBuilder* mem = reinterpret_cast<DataBuilder*>(data);
    if (!mem->writeAndAdvance(ConstDataRange(reinterpret_cast<const char*>(ptr),
                                             reinterpret_cast<const char*>(ptr) + realsize))
             .isOK()) {
        // Cause curl to generate a CURLE_WRITE_ERROR by returning a different number than how much
        // data there was to write.
        return 0;
    }

    return realsize;
}

CurlLibraryManager curlLibraryManager;
}  // namespace

std::unique_ptr<HttpClientInterface> createHttpClient() {
    curlLibraryManager.initialize();
    return stdx::make_unique<CurlHttpClient>();
}

StatusWith<std::size_t> CurlHttpClient::read(std::string path,
                                             DataRange buf,
                                             std::size_t offset,
                                             std::size_t count) const {
    std::string lastErr;
    std::string url(getSnapshotUrl(path, offset, count));
    std::string secretHeader(getSecretHeader());

    std::vector<int> kBackoffSleepDurations{1, 5, 10, 15, 20, 25, 30};

    for (std::size_t attempt = 0; attempt < kBackoffSleepDurations.size(); ++attempt) {
        std::unique_ptr<CURL, void (*)(CURL*)> myHandle(curl_easy_init(), curl_easy_cleanup);
        if (!myHandle) {
            return {ErrorCodes::InternalError, "Curl initialization failed"};
        }

        curl_easy_setopt(myHandle.get(), CURLOPT_URL, url.c_str());

        struct curl_slist* list = nullptr;
        const auto guard = MakeGuard([&] {
            if (list)
                curl_slist_free_all(list);
        });
        list = curl_slist_append(list, secretHeader.c_str());

        DataBuilder data(count);
        curl_easy_setopt(myHandle.get(), CURLOPT_HTTPHEADER, list);
        curl_easy_setopt(myHandle.get(), CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
        curl_easy_setopt(myHandle.get(), CURLOPT_WRITEDATA, &data);

        CURLcode result = curl_easy_perform(myHandle.get());
        if (result != CURLE_OK) {
            lastErr = str::stream() << "Bad HTTP response from API server: "
                                    << curl_easy_strerror(result);
            sleepsecs(kBackoffSleepDurations[attempt]);
            continue;
        }

        uassertStatusOK(buf.write(data.getCursor()));

        return {data.size()};
    }

    return {ErrorCodes::OperationFailed, lastErr};
}

}  // namespace mongo
