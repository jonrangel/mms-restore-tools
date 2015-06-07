# mms-restore-tools

Tools for end-to-end restore of data from the MongoDB Management Service (MMS) / MongoDB Ops Manager.

This repo contains the following tools:
- `restore_cluster.py`
- `chunkRemapper.js`

## restore_cluster.py

Performs an end-to-end restore of a sharded cluster to a target environment with a different topology (different number of shards).  The following are the key steps enacted by this script:

1. Trigger a full restore of the source sharded cluster using the MMS API
2. For each shard in the restore:
  1. Download the gzipped tarball of the MongoDB data files
  2. Decompress and extract the MongoDB data files from the tarball
  3. Spawn a transient `mongod` to stage access to the data files, and use `mongodump` to create a logical dump of the data
  4. Use `mongorestore` to load the data into the target environment

`restore_cluster.py` uses multiple threads in order to process multiple shards in parallel, and uses mulitple task queues in order to orchestrate the selection of source shards that are loaded into the target environment at any given time such that at all times, all shards in the target environment are being written to (thus greatly optimizing the time required for the bulk load into the target environment).

### Usage

`restore_cluster.py` may be used on its own but when used together with `chunkRemapper.js` optimal performance of the bulk load of data into the target environment can be achieved.  In order to do so, the user should ensure the following:

1. First, ensure that the 'shard mapping' logic in `chunkRemapper.js` (in `srcShardToDstShard()`) matches the equivalent logic in `restore_cluster.py` (in `task_queue_for_shard()`)
2. Run `chunkRemapper.js` to split and migrate chunks in the target collection to create a well-known mapping between source shards and destination shards.
3. Now that the mapping has been created, run `restore_cluster.py` to enact the restore.

The following global variables in `restore_cluster.py` should be configured:

| Variable                          | Description |
|-----------------------------------|-------------|
| MMS_USERNAME                      | MMS Username |
| MMS_PASSWORD                      | MMS API Key |
| MMS_BASE_URL                      | MMS Base URL e.g. 'https://mms.mongodb.com/' |
| POINT_IN_TIME_RESTORE             | Restore to a point in time or the most recent snapshot? |
| POINT_IN_TIME                     | The point in time (if a PIT restore is required) |
| SOURCE_GROUP_NAME                 | MMS group in which the source cluster resides |
| SOURCE_CLUSTER_NAME               | Name of the source cluster in MMS |
| SOURCE_SHARD_NAME_REGEX           | A regular expression matching shard names in the source cluster - shard names are expected to have a numerical extension that can be parsed by this regex |
| RESTORE_USERNAME                  | Username in target MongoDB environment |
| RESTORE_PASSWORD                  | Password in target MongoDB environment |
| RESTORE_SSL                       | Does the target environment require SSL connections? |
| TARGET_HOST_PORT_LIST             | List of (host,port) pairs for `mongos` in the target environment |
| NUM_TASK_QUEUES                   | The number of task queues to use. Set this to the number of shards in the target environment - after running `chunkRemapper.js`, each task queue should contain a restore job whose data maps to exactly one shard in the target environment (i.e. each task queue targets a different shard in the target environment) |
| NUM_WORKER_THREADS_PER_TASK_QUEUE | Number of worker threads per task queue, for additional concurrency |

### Dependencies

- Python 2.6 or Python 2.7
- `pymongo` - the MongoDB Python Driver
- Python `requests` module
- `curl`, `tar`, `mongodump` and `mongorestore` installed on the system where the script is run, and in the user's PATH
- `mongodump` and `mongorestore` version 3.0.0 or later

### Known Limitations / Areas for Improvement

- All config knobs should be exposed in such as way that they can be passed on the command line or in a separate (JSON-based) configuration file.
- No capability for filtering the restore (need to expose the `-d`, `-c` and `-q` options of `mongodump` at the logical dump stage)
- Script supports 'task queue orchestration' only for one target collection. What if there are multiple large sharded collections to restore - how can the bulk load performance be optimized for all of them?  Create one logical mongodump per collection?

## chunkRemapper.js

A `mongo` shell script to pre-split chunks in a newly sharded, empty, destination collection based on the chunk boundaries in a sharded source collection.

This script pre-splits and moves chunks in the target environment to create a well-known mapping of source shards to destination shards.  For example, suppose there is a collection "test.foo" in a source cluster with 6 shards and a in destination cluster with 2 shards.  The following mapping would be created:
- source shard 1 --> destination shard 1
- source shard 2 --> destination shard 2
- source shard 3 --> destination shard 1
- source shard 4 --> destination shard 2
- source shard 5 --> destination shard 1
- source shard 6 --> destination shard 2

where 'mapping' means that the 'test.foo' shard key ranges residing on the source shard are on the corresponding destination shard.

The well-known mapping created by this script can be exploited by `restore_cluster.py` to orchestrate the bulk load of data into the target environment such that all target shards are utilized during the bulk load.

### Usage

Run this against a mongos in the source cluster as a user that has permissions to read the `config` db.

The following global variables in `chunkRemapper.js` should be configured:

| Variable               | Description |
|------------------------|-------------|
| SRC_DATABASE_NAME      | The name of the database in the source cluster |
| SRC_COLLECTION_NAME    | The name of the collection in the source cluster |
| DST_DATABASE_NAME      | The name of the database in the destination cluster |
| DST_COLLECTION_NAME    | The name of the database in the destination cluster |
| SHARD_KEY              | Shard key (currently, only hashed shard keys are supported) |
| SHARD_KEY_MIN          | Shard Key MinKey |
| SHARD_KEY_MAX          | Shard Key MaxKey |
| DST_MONGOS             | hostname:port string for a mongos in the target cluster |
| DST_USERNAME           | Username for user in the destination cluster (must have `clusterAdmin` and `readWrite` roles in the target database) |
| DST_PASSWORD           | Password for user in the destination cluster |
| SRC_NUM_SHARDS         | Number of shards in the source cluster |
| DST_NUM_SHARDS         | Number of shards in the destination cluster |
| SRC_SHARD_NAME_REGEX   | A regular expression matching shard names in the source cluster - shard names are expected to have a numerical extension that can be parsed by this regex |
| DST_SHARD_NAME_PREFIX  | Prefix of shard names in the destination cluster |
| DST_SHARD_NAME_PADDING | Shard names in the destination cluster are expected to have a numerical suffix - this config knob indicates how many leading zeros are in the suffix |

### Dependencies

- MongoDB shell

### Known Limitations / Areas for Improvement

- Only supports collections with hashed shard keys (though adding support for range-based shard keys is easier!)
