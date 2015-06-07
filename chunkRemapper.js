/*
 * chunkRemapper.js
 *
 * A mongo shell script to pre-split chunks in a newly sharded, empty,
 * destination collection based on the chunk boundaries in a sharded 
 * source collection.
 * 
 * Run this against a mongos in the source cluster as a user that has 
 * permissions to read the 'config' db.
 *
 * Jon Rangel, March 2015
 */


/*
 * GLOBALS
 */

SRC_DATABASE_NAME = "database";
SRC_COLLECTION_NAME = "collection";
SRC_NAMESPACE = SRC_DATABASE_NAME + "." + SRC_COLLECTION_NAME;

DST_DATABASE_NAME = "database";
DST_COLLECTION_NAME = "collection";
DST_NAMESPACE = DST_DATABASE_NAME + "." + DST_COLLECTION_NAME;

SHARD_KEY = {"foo": "hashed"};
SHARD_KEY_MIN = {"foo": MinKey};
SHARD_KEY_MAX = {"foo": MaxKey};

// hostname:port string for a mongos in the target cluster
DST_MONGOS = "localhost:37017";

// credentials for user in the destination cluster
// must have clusterAdmin role and readWrite in the target database
DST_USERNAME = null;
DST_PASSWORD = null;

SRC_NUM_SHARDS = 288;
DST_NUM_SHARDS = 32;

SRC_SHARD_NAME_REGEX = new RegExp("source-shard_([0-9]+)");
DST_SHARD_NAME_PREFIX = "destination-shard";
DST_SHARD_NAME_PADDING = 2;

/*
 * FUNCTIONS
 */

splitCount = 0;
moveCount = 0;
noOpMoveCount = 0;

function doSplitChunk(split_point) {
    while (true) {
        // split in the destination collection using the lower bound of the chunk in the source collection
        // http://docs.mongodb.org/manual/reference/command/split/
        var res = dst_admin_db.runCommand({ "split": DST_NAMESPACE,
                                            "middle" : split_point });        
        if (res["ok"]) {
            //print("Split at: ");
            //printjson(split_point);
            splitCount++;
            break;
        } else {
            printjson(res);
            // if (res["errmsg"].match(/cannot split on initial or final chunk/) || // MongoDB 2.6
            //     res["errmsg"].match(/is a boundary key of existing chunk/)) {    // MongoDB 3.0
            //     break;
            // }

            // retry only if we get one of the following errmsg.
            if (res["errmsg"].match(/metadata lock/) != null ||
                res["errmsg"].match(/migrate already in progress/) != null) {
                // fall into the retry loop
            } else {
                // something fatal, let's assert
                assert(false);
            }
            print("Retrying split ...");
            sleep(1000);
        }
    }
}


// this function is for handling moves with a hashed shard key only.
function doMoveHashedChunk(lower_bound, upper_bound, dst_shard) {
    while (true) {
        // move the chunk specified by the lower and upper bounds to the destination shard
        // http://docs.mongodb.org/manual/reference/command/moveChunk/
        var res = dst_admin_db.runCommand({ "moveChunk": DST_NAMESPACE,
                                            "bounds": [lower_bound, upper_bound],
                                            "to": dst_shard });        
        if (res["ok"]) {
            //print("Moved chunk to shard " + dst_shard + ":");
            //printjson(lower_bound);
            //printjson(upper_bound);
            moveCount++;
            break;
        } else {
            printjson(res);
            // retry only if we get the "The collection's metadata lock is already taken." errmsg.
            if (res["errmsg"].match(/metadata lock/) != null ||
                res["errmsg"].match(/migrate already in progress/) != null) {
                // fall into the retry loop
            } else if (res["errmsg"].match(/that chunk is already on that shard/) != null) {
                // not an error, carry on
                noOpMoveCount++;
                break;
            } else {
                // something fatal, let's assert
                assert(false);
            }
            print("Retrying moveChunk ...");
            sleep(1000);
        }
    }
}


function srcShardToDstShard(src_shard_name) {
    pad_fn = function (num, size) {
        var s = num + "";
        while (s.length < size) s = "0" + s;
        return s;
    };

    var re = SRC_SHARD_NAME_REGEX;
    var m = src_shard_name.match(re);
    var src_shard_num = null;
    if (m != null) {
        src_shard_num = parseInt(m[1]);
    }
    assert(src_shard_num != null);

    dst_shard_num = (src_shard_num % DST_NUM_SHARDS) + 1;

    return DST_SHARD_NAME_PREFIX + pad_fn(dst_shard_num, DST_SHARD_NAME_PADDING);
}


/*
 * MAIN
 */

// get a connection to the destination cluster
var c = new Mongo(DST_MONGOS);

// get a handle to the admin db
var dst_admin_db = c.getDB("admin");

// authenticate if need be
if (DST_USERNAME != null) {
    var auth_result = dst_admin_db.auth(DST_USERNAME, DST_PASSWORD);
    assert(auth_result);
}

// drop the target collection and create an empty sharded one
var drop_result = c.getDB(DST_DATABASE_NAME).getCollection(DST_COLLECTION_NAME).drop();

dst_admin_db.runCommand({ "enableSharding": DST_DATABASE_NAME });
var res = dst_admin_db.runCommand({ "shardCollection": DST_NAMESPACE,
                                    "key": SHARD_KEY,
                                    "numInitialChunks": 1 });
//printjson(res);
assert(res["ok"]);


// stop the balancer while we create and move the chunks
// (we don't want to re-enable the balancer until after we've finished our subsequent bulk load)
c.getDB("config").settings.update({ "_id": "balancer" }, { "$set" : { "stopped": true } }, { "upsert": true });

// in the source cluster, read the chunks collection
// MongoDB ensures an index on { ns:1, min:1 }
var src_chunk_cursor = db.getSiblingDB("config").chunks.find({"ns":SRC_NAMESPACE}).sort({"min":1});
var prev_doc = null;
var curr_doc = null;
var count = 0;
var lower_bound = null;
var shard_check = null;
while (src_chunk_cursor.hasNext()) {
    prev_doc = curr_doc;
    curr_doc = src_chunk_cursor.next();

    if (prev_doc == null) {
        print("First time around - skipping");
        assert(curr_doc["_id"].match("MinKey") != null);
        lower_bound = SHARD_KEY_MIN;
        shard_check = curr_doc["shard"];
        //printjson(lower_bound);
        //print(shard_check);
        continue;
    }

    count++;

    if (count % 100 == 0) {
        print("Processed " + count + " input chunks");
    }

    // make sure to exlude $minKey - we don't want to use that as a parameter to the split command
    if (curr_doc["_id"].match("MinKey") != null) {
        print("Skipping MinKey chunk");
        continue;
    }

    // we want to minimize the time that this script takes to run, so avoid unnecessary chunk splits if
    // adjacent chunks are on the same shard.
    if (curr_doc["shard"] == prev_doc["shard"]) {
        continue;
    }

    doSplitChunk(curr_doc["min"]);
    assert(prev_doc["shard"] == shard_check);
    doMoveHashedChunk(lower_bound, curr_doc["min"], srcShardToDstShard(prev_doc["shard"]));

    // new lower bound to be used in the next chunk move
    lower_bound = curr_doc["min"];
    shard_check = curr_doc["shard"]
    //printjson(lower_bound);
    //print(shard_check);
}


assert(lower_bound != null && shard_check != null);
//print("Final move:");
doMoveHashedChunk(lower_bound, SHARD_KEY_MAX, srcShardToDstShard(shard_check));

print("Finished! Did " + splitCount + " splits and " + (moveCount + noOpMoveCount) + " moves (" + noOpMoveCount + " of those moves were no-ops)");
