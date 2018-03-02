/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/db/repl/replication_recovery.h"

#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replication_consistency_markers_impl.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/sync_tail.h"
#include "mongo/util/log.h"

namespace mongo {
namespace repl {

ReplicationRecoveryImpl::ReplicationRecoveryImpl(StorageInterface* storageInterface,
                                                 ReplicationConsistencyMarkers* consistencyMarkers)
    : _storageInterface(storageInterface), _consistencyMarkers(consistencyMarkers) {}

void ReplicationRecoveryImpl::recoverFromOplog(OperationContext* opCtx,
                                               Timestamp stableTimestamp) try {
    repl::RecoveryModeBlock recoveryBlock(opCtx);
    if (_consistencyMarkers->getInitialSyncFlag(opCtx)) {
        log() << "No recovery needed. Initial sync flag set.";
        return;  // Initial Sync will take over so no cleanup is needed.
    }

    const auto truncateAfterPoint = _consistencyMarkers->getOplogTruncateAfterPoint(opCtx);
    if (!truncateAfterPoint.isNull()) {
        log() << "Removing unapplied entries starting at: " << truncateAfterPoint.toBSON();
        _truncateOplogTo(opCtx, truncateAfterPoint);

        // Clear the truncateAfterPoint so that we don't truncate the next batch of oplog
        // entries
        // erroneously.
        _consistencyMarkers->setOplogTruncateAfterPoint(opCtx, {});
    }

    // If we were passed in a stable timestamp, we are in rollback recovery and should recover
    // from a stable timestamp. Otherwise, we're recovering at startup. If this storage engine
    // supports recover to stable timestamp, we may have shut down after taking a stable
    // checkpoint
    // and we ask it for the stable timestamp. Even if the storage engine supports recover to
    // stable timestamp, we may not be recovering from a stable checkpoint if we're upgrading,
    // in
    // which case the storage engine will return a null timestamp. In both this case, and a
    // storage
    // engine that does not support recover to a stable timestamp, we recover from an unstable
    // checkpoint.
    bool supportsRecoverToStableTimestamp =
        _storageInterface->supportsRecoverToStableTimestamp(opCtx->getServiceContext());
    if (stableTimestamp.isNull() && supportsRecoverToStableTimestamp) {
        stableTimestamp =
            _storageInterface->getLastStableCheckpointTimestamp(opCtx->getServiceContext());
    }

    auto topOfOplogSW = _getTopOfOplog(opCtx);
    boost::optional<OpTime> topOfOplog = boost::none;
    if (topOfOplogSW.getStatus() != ErrorCodes::CollectionIsEmpty &&
        topOfOplogSW.getStatus() != ErrorCodes::NamespaceNotFound) {
        fassertStatusOK(40290, topOfOplogSW);
        topOfOplog = topOfOplogSW.getValue();
    }

    const auto appliedThrough = _consistencyMarkers->getAppliedThrough(opCtx);

    if (stableTimestamp.isNull()) {
        _recoverFromUnstableCheckpoint(opCtx, topOfOplog, appliedThrough);
    } else {
        invariant(supportsRecoverToStableTimestamp);
        _recoverFromStableTimestamp(opCtx, stableTimestamp, topOfOplog, appliedThrough);
    }
} catch (...) {
    severe() << "Caught exception during replication recovery: " << exceptionToStatus();
    std::terminate();
}

/*
// Oplog is empty. There are no oplog entries to apply, so we exit recovery. If there was a
// checkpointTimestamp then we already set the initial data timestamp. Otherwise, there is
// nothing to set it to.
if (!topOfOplog) {
log() << "No oplog entries to apply for recovery. Oplog is empty.";
return;
}

if (auto startPoint = _getOplogApplicationStartPoint(checkpointTimestamp, appliedThrough)) {
log() << "Replication recovery start point: "
      << (startPoint ? startPoint->toString() : "boost::none");
_applyToEndOfOplog(opCtx, startPoint.get(), topOfOplog->getTimestamp());
}

// If we don't have a checkpoint timestamp, then we are either not running a storage engine
// that supports "recover to stable timestamp" or we just upgraded from a version that didn't.
// In both cases, the data on disk is not consistent until we have applied all oplog entries to
// the end of the oplog, since we do not know which ones actually got applied before shutdown.
// As a result, we do not set the initial data timestamp until after we have applied to the end
// of the oplog.
if (checkpointTimestamp.isNull()) {
_storageInterface->setInitialDataTimestamp(opCtx->getServiceContext(),
                                           topOfOplog->getTimestamp());
}
}
catch (...) {
severe() << "Caught exception during replication recovery: " << exceptionToStatus();
std::terminate();
}
*/

void ReplicationRecoveryImpl::_recoverFromStableTimestamp(OperationContext* opCtx,
                                                          Timestamp stableTimestamp,
                                                          boost::optional<OpTime> topOfOplog,
                                                          OpTime appliedThrough) {
    invariant(!stableTimestamp.isNull());
    std::string oplogString = topOfOplog ? topOfOplog->toString() : "empty";
    log() << "Recovering from stable timestamp: " << stableTimestamp
          << " (top of oplog: " << oplogString << ", appliedThrough: " << appliedThrough << ")";

    // If we recovered to a stable timestamp, we set the initial data timestamp now so that
    // the operations we apply below can be given the proper timestamps.
    _storageInterface->setInitialDataTimestamp(opCtx->getServiceContext(), stableTimestamp);

    // Oplog is empty. There are no oplog entries to apply, so we exit recovery and go into
    // initial sync.
    if (!topOfOplog) {
        log() << "No oplog entries to apply for recovery. Oplog is empty.";
        return;
    }

    // The writes to appliedThrough are given the timestamp of the end of the batch, and batch
    // boundaries are the only valid timestamps in which we could take checkpoints, so if you see
    // a non-null applied through in a stable checkpoint it must be at the same timestamp as the
    // checkpoint.

    /*
        invariant(appliedThrough.isNull() || appliedThrough.getTimestamp() == stableTimestamp,
                  str::stream() << "appliedThrough (" << appliedThrough.toString()
                                << ") does not equal stable timestamp ("
                                << stableTimestamp.toString()
                                << ")");
    */

    log() << "Starting recovery oplog application at the stable timestamp: " << stableTimestamp;
    _applyToEndOfOplog(opCtx, stableTimestamp, topOfOplog->getTimestamp());
}

void ReplicationRecoveryImpl::_recoverFromUnstableCheckpoint(OperationContext* opCtx,
                                                             boost::optional<OpTime> topOfOplog,
                                                             OpTime appliedThrough) {
    std::string oplogString = topOfOplog ? topOfOplog->toString() : "empty";
    log() << "Recovering from an unstable checkpoint (top of oplog: " << oplogString
          << ", appliedThrough: " << appliedThrough << ")";
    // Oplog is empty. There are no oplog entries to apply, so we exit recovery and go into
    // initial sync.
    if (!topOfOplog) {
        log() << "No oplog entries to apply for recovery. Oplog is empty.";
        return;
    }

    if (appliedThrough.isNull()) {
        // The appliedThrough would be null if we shut down cleanly or crashed as a primary. Either
        // way we are consistent at the top of the oplog.
        log() << "No oplog entries to apply for recovery. appliedThrough is null.";
    } else {
        // If the appliedThrough is not null, then we shut down uncleanly during secondary oplog
        // application and must apply from the appliedThrough to the top of the oplog.
        log() << "Starting recovery oplog application at the appliedThrough: " << appliedThrough
              << ", through the top of the oplog: " << oplogString;
        _applyToEndOfOplog(opCtx, appliedThrough.getTimestamp(), topOfOplog->getTimestamp());
    }

    // The data on disk is not consistent until we have applied all oplog entries to the end of the
    // oplog, since we do not know which ones actually got applied before shutdown. As a result,
    // we do not set the initial data timestamp until after we have applied to the end of the oplog.
    _storageInterface->setInitialDataTimestamp(opCtx->getServiceContext(),
                                               topOfOplog->getTimestamp());
}

void ReplicationRecoveryImpl::_applyToEndOfOplog(OperationContext* opCtx,
                                                 Timestamp oplogApplicationStartPoint,
                                                 Timestamp topOfOplog) {
    invariant(!oplogApplicationStartPoint.isNull());
    invariant(!topOfOplog.isNull());

    // Check if we have any unapplied ops in our oplog. It is important that this is done after
    // deleting the ragged end of the oplog.
    if (oplogApplicationStartPoint == topOfOplog) {
        log() << "No oplog entries to apply for recovery. appliedThrough is at the top of the "
                 "oplog.";
        return;  // We've applied all the valid oplog we have.
    } else if (oplogApplicationStartPoint > topOfOplog) {
        severe() << "Applied op " << oplogApplicationStartPoint.toBSON()
                 << " not found. Top of oplog is " << topOfOplog.toBSON() << '.';
        fassertFailedNoTrace(40313);
    }

    log() << "Replaying stored operations from " << oplogApplicationStartPoint.toBSON()
          << " (exclusive) to " << topOfOplog.toBSON() << " (inclusive).";

    DBDirectClient db(opCtx);
    auto cursor = db.query(NamespaceString::kRsOplogNamespace.ns(),
                           QUERY("ts" << BSON("$gte" << oplogApplicationStartPoint)),
                           /*batchSize*/ 0,
                           /*skip*/ 0,
                           /*projection*/ nullptr,
                           QueryOption_OplogReplay);

    // Check that the first document matches our appliedThrough point then skip it since it's
    // already been applied.
    if (!cursor->more()) {
        // This should really be impossible because we check above that the top of the oplog is
        // strictly > appliedThrough. If this fails it represents a serious bug in either the
        // storage engine or query's implementation of OplogReplay.
        severe() << "Couldn't find any entries in the oplog >= "
                 << oplogApplicationStartPoint.toBSON() << " which should be impossible.";
        fassertFailedNoTrace(40293);
    }

    auto firstTimestampFound =
        fassertStatusOK(40291, OpTime::parseFromOplogEntry(cursor->nextSafe())).getTimestamp();
    if (firstTimestampFound != oplogApplicationStartPoint) {
        severe() << "Oplog entry at " << oplogApplicationStartPoint.toBSON()
                 << " is missing; actual entry found is " << firstTimestampFound.toBSON();
        fassertFailedNoTrace(40292);
    }

    // Apply remaining ops one at at time, but don't log them because they are already logged.
    UnreplicatedWritesBlock uwb(opCtx);
    DisableDocumentValidation validationDisabler(opCtx);

    OpTime lastApplied;
    BSONObj entry;
    while (cursor->more()) {
        invariant(!opCtx->lockState()->inAWriteUnitOfWork());
        entry = cursor->nextSafe();
        // We do not ignore many constraints here. If we are recovering from a stable timestamp
        // then we began recovery consistent and we expect all oplog entries to apply without error.
        // Otherwise, we can only be replaying a single batch of oplog entries, and no constraints
        // can be violated within a single batch.
        fassertStatusOK(40294,
                        SyncTail::syncApply(opCtx, entry, OplogApplication::Mode::kRecovering));
        // if (res != ErrorCodes::NamespaceNotFound) {
        //     fassert
        // } else {
        //     log() << "Swallowing error during replication recovery: " << res.toString();
        // }
    }

    // If we crash before setting appliedThrough, we will either recover to the same stable
    // timestamp at a consistent point, or we are only applying one single batch and it is safe to
    // replay the batch from any point.
    auto entryTs = fassertStatusOK(40295, OpTime::parseFromOplogEntry(entry));
    _consistencyMarkers->setAppliedThrough(opCtx, entryTs);
    // _storageInterface->setStableTimestamp(opCtx->getServiceContext(), entryTs.getTimestamp());
}

StatusWith<OpTime> ReplicationRecoveryImpl::_getTopOfOplog(OperationContext* opCtx) const {
    const auto docsSW = _storageInterface->findDocuments(opCtx,
                                                         NamespaceString::kRsOplogNamespace,
                                                         boost::none,  // Collection scan
                                                         StorageInterface::ScanDirection::kBackward,
                                                         {},
                                                         BoundInclusion::kIncludeStartKeyOnly,
                                                         1U);
    if (!docsSW.isOK()) {
        return docsSW.getStatus();
    }
    const auto docs = docsSW.getValue();
    if (docs.empty()) {
        return Status(ErrorCodes::CollectionIsEmpty, "oplog is empty");
    }
    invariant(1U == docs.size());

    return OpTime::parseFromOplogEntry(docs.front());
}

void ReplicationRecoveryImpl::_truncateOplogTo(OperationContext* opCtx,
                                               Timestamp truncateTimestamp) {
    const NamespaceString oplogNss(NamespaceString::kRsOplogNamespace);
    AutoGetDb autoDb(opCtx, oplogNss.db(), MODE_IX);
    Lock::CollectionLock oplogCollectionLoc(opCtx->lockState(), oplogNss.ns(), MODE_X);
    Collection* oplogCollection = autoDb.getDb()->getCollection(opCtx, oplogNss);
    if (!oplogCollection) {
        fassertFailedWithStatusNoTrace(
            34418,
            Status(ErrorCodes::NamespaceNotFound,
                   str::stream() << "Can't find " << NamespaceString::kRsOplogNamespace.ns()));
    }

    // Scan through oplog in reverse, from latest entry to first, to find the truncateTimestamp.
    RecordId oldestIDToDelete;  // Non-null if there is something to delete.
    auto oplogRs = oplogCollection->getRecordStore();
    auto oplogReverseCursor = oplogRs->getCursor(opCtx, /*forward=*/false);
    size_t count = 0;
    while (auto next = oplogReverseCursor->next()) {
        const BSONObj entry = next->data.releaseToBson();
        const RecordId id = next->id;
        count++;

        const auto tsElem = entry["ts"];
        if (count == 1) {
            if (tsElem.eoo())
                LOG(2) << "Oplog tail entry: " << redact(entry);
            else
                LOG(2) << "Oplog tail entry ts field: " << tsElem;
        }

        if (tsElem.timestamp() < truncateTimestamp) {
            // If count == 1, that means that we have nothing to delete because everything in
            // the
            // oplog is < truncateTimestamp.
            if (count != 1) {
                invariant(!oldestIDToDelete.isNull());
                oplogCollection->cappedTruncateAfter(opCtx, oldestIDToDelete, /*inclusive=*/true);
            }
            return;
        }

        oldestIDToDelete = id;
    }

    severe() << "Reached end of oplog looking for oplog entry before " << truncateTimestamp.toBSON()
             << " but couldn't find any after looking through " << count << " entries.";
    fassertFailedNoTrace(40296);
}

}  // namespace repl
}  // namespace mongo
