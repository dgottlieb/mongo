/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_index.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_parameters.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_server_status.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {

class S3WiredTiger final : public StorageEngine::Factory {
public:
    ~S3WiredTiger() override {}
    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {

        size_t cacheMB = WiredTigerUtil::getCacheSizeMB(wiredTigerGlobalOptions.cacheSizeGB);
        const bool kEphemeral = false;

        uassert(ErrorCodes::InvalidOptions,
                "--dbpath cannot contain quotes",
                params.dbpath.find('"') == std::string::npos);

        std::string fsOptions = str::stream()
            << "local={entry=s3WtFsCreate,early_load=true,config={}}";

        WiredTigerExtensions::get(getGlobalServiceContext())->addExtension(fsOptions);

        WiredTigerKVEngine* kv =
            new WiredTigerKVEngine(getCanonicalName().toString(),
                                   params.dbpath,
                                   getGlobalServiceContext()->getFastClockSource(),
                                   wiredTigerGlobalOptions.engineConfig,
                                   cacheMB,
                                   params.dur,
                                   kEphemeral,
                                   params.repair,
                                   params.readOnly);
        kv->setRecordStoreExtraOptions(wiredTigerGlobalOptions.collectionConfig);
        kv->setSortedDataInterfaceExtraOptions(wiredTigerGlobalOptions.indexConfig);
        // Intentionally leaked.
        new WiredTigerServerStatusSection(kv);
        new WiredTigerEngineRuntimeConfigParameter(kv);

        KVStorageEngineOptions options;
        options.directoryPerDB = params.directoryperdb;
        options.directoryForIndexes = wiredTigerGlobalOptions.directoryForIndexes;
        options.forRepair = params.repair;
        return new KVStorageEngine(kv, options);
    }

    StringData getCanonicalName() const override {
        return "s3_wt";
    }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const override {
        Status status =
            metadata.validateStorageEngineOption("directoryPerDB", params.directoryperdb);
        if (!status.isOK()) {
            return status;
        }

        status = metadata.validateStorageEngineOption("directoryForIndexes",
                                                      wiredTigerGlobalOptions.directoryForIndexes);
        if (!status.isOK()) {
            return status;
        }

        const bool kDefaultGroupCollections = false;
        status =
            metadata.validateStorageEngineOption("groupCollections",
                                                 params.groupCollections,
                                                 boost::optional<bool>(kDefaultGroupCollections));
        if (!status.isOK()) {
            return status;
        }

        return Status::OK();
    }

    BSONObj createMetadataOptions(const StorageGlobalParams& params) const override {
        BSONObjBuilder builder;
        builder.appendBool("directoryPerDB", params.directoryperdb);
        return builder.obj();
    }

    bool supportsReadOnly() const override {
        return true;
    }
};

}  // namespace

MONGO_INITIALIZER_WITH_PREREQUISITES(S3WiredTiger, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("s3_wt", new S3WiredTiger());
    return Status::OK();
}

}  // namespace mongo
