TestData.skipCheckDBHashes = true;
var rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

let shell = startParallelShell(function() {
    db = db.getSiblingDB('test');
    db.bla.ensureIndex({x: 1});
    let i = 0

        while (true) {
        try {
            var bulk = db.bla.initializeUnorderedBulkOp();
            for (var num = 0; num < 1000; ++num) {
                bulk.insert({x: i++});
            };
            bulk.execute();
        } catch (e) {
            jsTestLog("DONE PRIMARY: " + e);
            db.adminCommand({shutdown: 1, force: true});
        }
    }
}, rst.getPrimary().port);

sleep(4 * 1000);
rst.stop(1);
var node1 = rst.restart(1, {noReplSet: true});  // Restart as a standalone node.
let coll = node1.getCollection('test.bla');

q = {};
coll.find().forEach(function(x) {
    if (q[x["_id"]]) {
        jsTestLog("Duplicate _id found (no sort): " + tojson(x));
    }
    q[x["_id"]] = x["_id"]
});

w = {};
coll.find().sort({_id: 1}).forEach(function(x) {
    if (w[x["_id"]]) {
        jsTestLog("Duplicate _id found: " + tojson(x));
    }
    w[x["_id"]] = x["_id"]
});

diff0 = [];
for (var key in q) {
    if (!w.hasOwnProperty(key)) {
        diff0.push(q[key]);
    }
}

diff1 = [];
for (var key in w) {
    if (!q.hasOwnProperty(key)) {
        diff1.push(w[key]);
    }
}

jsTestLog("DONE SECONDARY");
jsTestLog(tojson(diff0));
jsTestLog(tojson(diff1));

var diffLogs0 = [];
diff0.forEach(function(d) {
    let entry = node1.getCollection('local.oplog.rs').findOne({'o._id': d});
    diffLogs0.push(entry);
    jsTestLog(tojson(
        node1.getCollection('local.oplog.rs').find({ts: {$gte: entry.ts}}).limit(10).toArray()))
});
jsTestLog(tojson(diffLogs0));
var diffLogs1 = [];
diff1.forEach(function(d) {
    let entry = node1.getCollection('local.oplog.rs').findOne({'o._id': d});
    diffLogs1.push(entry);
    jsTestLog(tojson(
        node1.getCollection('local.oplog.rs').find({ts: {$gte: entry.ts}}).limit(10).toArray()))
});
jsTestLog(tojson(diffLogs1));

let val = coll.validate({full: true});
jsTestLog(tojson(val));
jsTestLog(tojson(node1.getDB('local').replset.minvalid.findOne()));
rst.dumpOplog(node1, {}, 30);

shell({checkExitSuccess: false});

assert(val.valid);
rst.stopSet();