TestData.skipCheckDBHashes = true;
var rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

let shell = startParallelShell(function() {
    db = db.getSiblingDB('test');
    db.bla.ensureIndex({x: 1});

    var doc = 'L'.repeat(50);

    while (true) {
        try {
            var bulk = db.bla.initializeUnorderedBulkOp();
            for (var num = 0; num < 1000; ++num) {
                bulk.insert({doc: doc, x: Math.floor(Math.random() * 1000000)});
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
    q[x["_id"]] = 1
});

w = {};
coll.find().sort({_id: 1}).forEach(function(x) {
    w[x["_id"]] = 1
});

diff = {};
for (var key in q) {
    if (!w.hasOwnProperty(key)) {
        diff[key] = 1;
    }
}

jsTestLog("DONE SECONDARY");
let validate = coll.validate();
jsTestLog(tojson({
    diff: diff,
    validate: validate,
    minValid: node1.getDB("local").replset.minvalid.find().next(),
    SUCCESS: validate['valid'],
}));

shell({checkExitSuccess: false});

assert(validate['valid']);
rst.stopSet();
