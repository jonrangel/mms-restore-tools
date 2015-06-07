#!/usr/bin/env python

from datetime import datetime, timedelta
import dateutil.parser
import json
import logging
import os
import pymongo
import random
import re
import requests
from requests.auth import HTTPDigestAuth

try:
    import subprocess32 as subprocess
except ImportError:
    print("WARNING: subprocess32 module not available; falling back to subprocess module (see https://docs.python.org/2/library/subprocess.html)")
    import subprocess

import sys
import threading
import time
import Queue
import urlparse


#
# CONSTANTS
#

# Restore to a specific point in time or the last snapshot?
POINT_IN_TIME_RESTORE = False
POINT_IN_TIME = datetime.now() - timedelta(hours=22)

# Primarily to help with testing this script -
# can specify an existing batch id for a cluster restore requested previously
# this saves on cluttering up the MMS restore jobs page unnecessarily
USE_EXISTING_RESTORE_JOBS = False
EXISTING_BATCH_ID = "5513d2eb498ed01959c9c499"

# MMS credentials
MMS_USERNAME = 'jon.rangel@10gen.com'
MMS_PASSWORD = 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
MMS_BASE_URL = 'https://mms.mongodb.com/'

SOURCE_GROUP_NAME = 'Rangel4'
SOURCE_CLUSTER_NAME = 'source'
SOURCE_SHARD_NAME_REGEX = r'source-shard_([0-9]+)'

# Credentials for target environment to be restored - leave empty if the target environment does not use auth
RESTORE_USERNAME = ''
RESTORE_PASSWORD = ''

# Does the target environment require SSL connections?
RESTORE_SSL = False

# Array of mongos in target cluster
TARGET_HOST_PORT_LIST = [('localhost','27017')]

# the number of task queues to use
# should probably set this to the number of shards in the target environment -
# after running chunkRemapper.js, each task queue should
# contain a restore job whose data maps to exactly one shard in the target environment
NUM_TASK_QUEUES = 2
NUM_WORKER_THREADS_PER_TASK_QUEUE = 5

# number of times to retry a failed mongorestore
MONGORESTORE_RETRY_ATTEMPTS = 3

# Eligible ports for transient mongods used at the mongodump stage
PORT_OFFSET = 1
PORT_BASE = 30000

# used for naming log files
SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
SCRIPT_START_TIME_STR = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')

# MMS API endpoints
MMS_API_BASE_URL = MMS_BASE_URL + 'api/public/v1.0/'
MMS_GROUPS_URL = MMS_API_BASE_URL + 'groups'
MMS_CLUSTERS_URL_FMT =  MMS_GROUPS_URL + '/{0}/clusters'
MMS_SNAPSHOTS_URL_FMT = MMS_GROUPS_URL + '/{0}/clusters/{1}/snapshots'
MMS_RESTORE_JOBS_URL_FMT = MMS_GROUPS_URL + '/{0}/clusters/{1}/restoreJobs'
MMS_RESTORE_JOBS_BATCHID_URL_FMT = MMS_GROUPS_URL + '/{0}/clusters/{1}/restoreJobs?batchId={2}'


#
# FUNCTIONS - Main Thread
#

def mms_api_get(url):
    response = requests.get(url, auth=HTTPDigestAuth(MMS_USERNAME, MMS_PASSWORD), verify=False)

    data = json.loads(response.text)
    if response.status_code == 200:
        logger.debug('GET request on {0} succeeded'.format(url))
        return data
    else:
        logger.error('GET request on {0} failed'.format(url))
        raise Exception(response.status_code, data['detail'])


def mms_api_post(url, data):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=json.dumps(data), headers=headers, auth=HTTPDigestAuth(MMS_USERNAME, MMS_PASSWORD), verify=False)

    data = json.loads(response.text)
    if response.status_code == 200:
        logger.debug('POST request on {0} succeeded'.format(url))
        return data
    else:
        logger.error('POST request on {0} failed'.format(url))
        raise Exception(response.status_code, data['detail'])


# yields results from api endpoints that return a list of entities
# see: https://docs.opsmanager.mongodb.com//current/core/api/#lists
def mms_api_entity_generator(entity_name, url):
    total_entity_count = None
    entities_returned = 0
    more_entities = True
    page_num = 1

    q = '&' if urlparse.urlparse(url).query else '?'

    while more_entities:
        response = mms_api_get(url + q + 'pageNum={0}&itemsPerPage={1}'.format(page_num, 100))

        if total_entity_count is None:
            total_entity_count = response['totalCount']
            logger.debug('Fetched {0} {1}'.format(total_entity_count, entity_name))

        for entity in response['results']:
            entities_returned += 1
            yield entity

        page_num += 1
        if next((link for link in response["links"] if link["rel"] == "next"), None) is None:
            more_entities = False

    assert total_entity_count == entities_returned


def mms_group_name_to_id(group_name):
    group_id = None

    mms_groups = mms_api_entity_generator('groups', MMS_GROUPS_URL)

    for group in mms_groups:
        if group['name'] == group_name:
            group_id = group['id']
            break

    return group_id


def mms_sharded_cluster_name_to_id(group_id, cluster_name):
    cluster_id = None

    mms_clusters = mms_api_entity_generator('clusters', MMS_CLUSTERS_URL_FMT.format(group_id))

    for cluster in mms_clusters:
        if cluster['typeName'] == 'SHARDED_REPLICA_SET' and cluster['clusterName'] == cluster_name:
            cluster_id = cluster['id']
            break

    return cluster_id


def mms_get_last_snapshot_id(group_id, cluster_id):
    latest_snapshot = None

    mms_snapshots = mms_api_entity_generator('snapshots', MMS_SNAPSHOTS_URL_FMT.format(group_id, cluster_id))

    for snapshot in mms_snapshots:
        if latest_snapshot is None or dateutil.parser.parse(snapshot['created']['date']) > dateutil.parser.parse(latest_snapshot['created']['date']):
            latest_snapshot = snapshot

    return latest_snapshot['id']


def mms_sharded_cluster_snapshot_restore(group_id, cluster_id, snapshot_id):
    logger.info('Requesting snapshot restore of cluster {0} in MMS group {1}'.format(cluster_id, group_id))

    snapshot = { "snapshotId": snapshot_id }

    logger.debug(json.dumps(snapshot))

    restore_job = mms_api_post(MMS_RESTORE_JOBS_URL_FMT.format(group_id, cluster_id), snapshot)

    assert 'batchId' in restore_job['results'][0] # we only handle sharded clusters in this script

    return restore_job['results'][0]['batchId']


def mms_sharded_cluster_pit_restore(group_id, cluster_id, ts):
    logger.info('Requesting point-in-time restore of cluster {0} in MMS group {1}'.format(cluster_id, group_id))

    point_in_time = {
        "timestamp": { "date": ts.strftime('%Y-%m-%dT%H:%M:%SZ'), "increment": 0 }
    }

    logger.debug(json.dumps(point_in_time))

    restore_job = mms_api_post(MMS_RESTORE_JOBS_URL_FMT.format(group_id, cluster_id), point_in_time)

    assert restore_job['results'][0]['pointInTime']
    assert restore_job['results'][0]['statusName'] == 'IN_PROGRESS'
    assert 'batchId' in restore_job['results'][0] # we only handle sharded clusters in this script

    return restore_job['results'][0]['batchId']


# It is critical that the logic here for mapping shards from the source cluster
# to task queues is the same as the logic in chunkRemapper.js
# that maps shards in the source cluster to shards in the destination cluster
def task_queue_for_shard(url):
    filename = url_to_filename(url)

    assert len(task_queues) == NUM_TASK_QUEUES

    match = re.search(SOURCE_SHARD_NAME_REGEX, filename)
    if match:
        source_shard_num  = int(match.group(1))
        task_queue_idx = source_shard_num % NUM_TASK_QUEUES
        logger.debug('Mapping {0} to task queue {1}'.format(filename, task_queue_idx))
    else:
        # The filename didn't match the regex - this is expected in the case of the config server restore job.
        # Just send it to the first task queue
        task_queue_idx = 0
        logger.debug('No regex match. Mapping {0} to default task queue {1}'.format(filename, task_queue_idx))

    return task_queues[task_queue_idx]


def check_and_enqueue_restore_urls(group_id, cluster_id, batch_id):
    while True:
        in_progress_job_count = 0
        finished_job_count = 0

        restore_jobs = mms_api_entity_generator('restore jobs',
                                                MMS_RESTORE_JOBS_BATCHID_URL_FMT.format(group_id, cluster_id, batch_id))
        for job in restore_jobs:
            status = job['statusName']
            if status == 'IN_PROGRESS':
                in_progress_job_count += 1
            elif status == 'FINISHED':
                finished_job_count += 1
                #assert job['delivery']['statusName'] == 'READY'
                url = job['delivery']['url']
                if url not in enqueued_restore_urls:
                    global PORT_OFFSET
                    port = PORT_BASE + PORT_OFFSET
                    PORT_OFFSET += 1

                    enqueued_restore_urls[url] = port

                    task = {'url': url, 'port': port}
                    task_queue_for_shard(url).put(task)
                    logger.debug('Enqueued URL {0}, port {1} for processing'.format(url, port))
            else:
                raise Exception(status)

        logger.debug('Current status of MMS restore jobs: {0} finished, {1} in progress ({2} total)'.format(
                     finished_job_count, in_progress_job_count, finished_job_count + in_progress_job_count))
        if in_progress_job_count == 0:
            break

        time.sleep(5)


def init_logging():
    # output to a file
    file_log_handler = logging.FileHandler('{0}.log.{1}'.format(SCRIPT_NAME, SCRIPT_START_TIME_STR))
    logger.addHandler(file_log_handler)

    # also output to stderr
    stderr_log_handler = logging.StreamHandler()
    logger.addHandler(stderr_log_handler)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    stderr_log_handler.setFormatter(formatter)


#
# FUNCTIONS - Worker Threads
#

def url_to_filename(url):
    return url.split('/')[-1]


def filename_to_dbpath(filename):
    return filename.split('.')[0]


def dbpath_to_mongodump_path(dbpath):
    return dbpath + '-BSON'


def download(url):
    filename = url_to_filename(url)
    logger.info('Starting download of {0}'.format(filename))
    p = subprocess.Popen(['curl', '-kOLsS', url], stderr=subprocess.STDOUT, stdout=None)
    p.wait()
    logger.info('Downloaded {0}'.format(filename))


def uncompress(filename):
    logger.info('Uncompressing {0}'.format(filename))
    p = subprocess.Popen(['tar', '-zxvf', filename], stderr=subprocess.STDOUT, stdout=None)
    p.wait()
    logger.info('Uncompressed {0}'.format(filename))
    p = subprocess.Popen(['rm', '-f', filename], stderr=subprocess.STDOUT, stdout=None)
    p.wait()


def mongodump(dbpath, port):
    mongod_log = open('{0}.mongod.log.{1}'.format(dbpath, SCRIPT_START_TIME_STR), 'w')
    mongod_command = 'mongod --dbpath {0} --port {1} --nojournal'.format(dbpath, port)

    logger.info('Starting mongod on port {0} ...'.format(port))
    p0 = subprocess.Popen(mongod_command, stderr=subprocess.STDOUT, stdout=mongod_log, shell=True)

    # wait until the mongod is ready
    while True:
        try:
            client = pymongo.MongoClient('localhost', int(port), connectTimeoutMS=50)
            break
        except pymongo.errors.ConnectionFailure:
            time.sleep(1)

    logger.info("Successfully established connection to mongod")

    mongodumprestore_log = open('{0}.mongodumprestore.log.{1}'.format(dbpath, SCRIPT_START_TIME_STR), 'w')
    mongodumprestore_log.write('STARTING AT {0}\n'.format(datetime.now()))
    mongodumprestore_log.close()

    mongodumprestore_log = open('{0}.mongodumprestore.log.{1}'.format(dbpath, SCRIPT_START_TIME_STR), 'a')
    mongodump_command = 'mongodump --forceTableScan --port ' + str(port) + ' --out ' + dbpath_to_mongodump_path(dbpath)

    logger.info('Starting mongodump of {0}'.format(dbpath))
    p1 = subprocess.Popen(mongodump_command, stderr=subprocess.STDOUT, stdout=mongodumprestore_log, shell=True)
    p1.wait()
    logger.info('Finished mongodump of {0}'.format(dbpath))
    mongodumprestore_log.close()

    # shutdown the mongod as it is no longer needed
    p0.terminate()
    p0.wait()
    mongod_log.close()

    # remove the config and admin databases from the dump if present
    md = dbpath_to_mongodump_path(dbpath)
    p = subprocess.Popen(['rm', '-rf', md + '/config', md + '/admin'], stderr=subprocess.STDOUT, stdout=None)
    p.wait()


def mongorestore(dbpath):
    global failed_jobs

    # choose a mongos in the target cluster at random to distribute load across mongos processes
    target_host_port = random.choice(TARGET_HOST_PORT_LIST)
    target_host = target_host_port[0]
    target_port = target_host_port[1]

    mongorestore_options = '--host {0} --port {1} --numInsertionWorkersPerCollection 32 --writeConcern {2}'.format(target_host, target_port, '{w:1}')
    if RESTORE_USERNAME != '':
        mongorestore_options += ' --username {0} --password {1} --authenticationDatabase=admin'.format(RESTORE_USERNAME, RESTORE_PASSWORD)
    if RESTORE_SSL:
        mongorestore_options += ' --ssl'
    mongorestore_command = 'mongorestore {0} {1}'.format(mongorestore_options, dbpath_to_mongodump_path(dbpath))

    success = False
    retries = 0
    logger.info('Starting mongorestore of {} to {}:{}'.format(dbpath_to_mongodump_path(dbpath), target_host, target_port))

    while not success:
        if retries >= MONGORESTORE_RETRY_ATTEMPTS:
            logger.error('Reached max number of mongorestore retry attempts...')
            break

        mongodumprestore_log = open('{0}.mongodumprestore.log.{1}'.format(dbpath, SCRIPT_START_TIME_STR), 'a')
        p = subprocess.Popen(mongorestore_command, stderr=subprocess.STDOUT, stdout=mongodumprestore_log, shell=True)
        p.wait()
        mongodumprestore_log.close()
        if p.returncode != 0:
            logger.info('Retrying mongorestore due to problem...')
            with failed_jobs_lock:
                failed_jobs[dbpath] = target_host + ':' + target_port
            retries += 1
            continue
        success = True
        logger.info('Finished mongorestore of {} to {}:{}'.format(dbpath_to_mongodump_path(dbpath), target_host, target_port))

    p = subprocess.Popen(['rm', '-rf', dbpath, dbpath_to_mongodump_path(dbpath)], stderr=subprocess.STDOUT, stdout=None)
    p.wait()

    mongodumprestore_log = open('{0}.mongodumprestore.log.{1}'.format(dbpath, SCRIPT_START_TIME_STR), 'a')
    mongodumprestore_log.write('ENDING AT {0}\n'.format(datetime.now()))
    mongodumprestore_log.close()


def process_url(url, port):
    filename = url_to_filename(url)
    dbpath = filename_to_dbpath(filename)

    download(url)
    uncompress(filename)
    mongodump(dbpath, port)
    mongorestore(dbpath)


def worker_thread_main(task_queue):
    while True:
        task = task_queue.get()
        logger.debug('Dequeued URL {0}, port {1} for processing'.format(task['url'], task['port']))
        process_url(task['url'], task['port'])
        logger.debug('Finished processing URL {0}, port {1}'.format(task['url'], task['port']))
        task_queue.task_done()


#
# GLOBALS
#
enqueued_restore_urls = {}

# list of task queues - one queue for each shard in the target environment
task_queues = []

logger = logging.getLogger(SCRIPT_NAME)

failed_jobs = {}
failed_jobs_lock = threading.Lock()


#
# MAIN
#
if __name__ == "__main__":
    # initialize logging
    init_logging()

    # create the task queues and start a worker thread pool for each queue
    for i in range(NUM_TASK_QUEUES):
        task_queues.append(Queue.Queue())
        for j in range(NUM_WORKER_THREADS_PER_TASK_QUEUE):
            t = threading.Thread(target=worker_thread_main, args=(task_queues[i],))
            t.daemon = True
            t.start()
    logger.debug('Started thread pool of {0} threads for each of {1} task queues'.format(NUM_WORKER_THREADS_PER_TASK_QUEUE,
                                                                                         NUM_TASK_QUEUES))

    # get the MMS group id for subsequent MMS API calls
    mms_group_id = mms_group_name_to_id(SOURCE_GROUP_NAME)

    # get the MMS cluster id for the source sharded cluster
    mms_cluster_id = mms_sharded_cluster_name_to_id(mms_group_id, SOURCE_CLUSTER_NAME)

    if USE_EXISTING_RESTORE_JOBS:
        batch_id = EXISTING_BATCH_ID
    elif POINT_IN_TIME_RESTORE:
        # initiate the point-in-time restore for the sharded cluster
        batch_id = mms_sharded_cluster_pit_restore(mms_group_id, mms_cluster_id, POINT_IN_TIME)
    else:
        # initiate restore from the most recent snapshot
        mms_snapshot_id = mms_get_last_snapshot_id(mms_group_id, mms_cluster_id)
        batch_id = mms_sharded_cluster_snapshot_restore(mms_group_id, mms_cluster_id, mms_snapshot_id)

    # poll status of restore jobs until all are complete
    check_and_enqueue_restore_urls(mms_group_id, mms_cluster_id, batch_id)

    # block until all tasks are done
    logger.info('All download URLs enqueued to worker threads.  Waiting until done...')
    for i in range(NUM_TASK_QUEUES):
        task_queues[i].join()

    # check for and report on any failed jobs
    if len(failed_jobs) > 0:
        logger.error('The following mongorestore jobs failed:')
        for job, target in failed_jobs.iteritems():
            logger.error(job + ' on ' + target)
        exit(1)

    logger.info('Our job here is done!')
