import json
import time
from collections import namedtuple
from typing import Callable, Dict, Iterator, List

import requests
from decorator import decorator
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.settings import BucketSettings, ClusterSpec

MAX_RETRY = 20
RETRY_DELAY = 10

ANALYTICS_PORT = 8095
EVENTING_PORT = 8096


@decorator
def retry(method: Callable, *args, **kwargs):
    r = namedtuple('request', ['url'])('')
    for _ in range(MAX_RETRY):
        try:
            r = method(*args, **kwargs)
        except ConnectionError:
            time.sleep(RETRY_DELAY * 2)
            continue
        if r.status_code in range(200, 203):
            return r
        else:
            logger.warn(r.text)
            logger.warn('Retrying {}'.format(r.url))
            time.sleep(RETRY_DELAY)
    logger.interrupt('Request {} failed after {} attempts'.format(
        r.url, MAX_RETRY
    ))


class RestHelper:

    def __init__(self, cluster_spec: ClusterSpec):
        self.rest_username, self.rest_password = cluster_spec.rest_credentials
        self.auth = self.rest_username, self.rest_password
        self.cluster_spec = cluster_spec

    @retry
    def get(self, **kwargs) -> requests.Response:
        return requests.get(auth=self.auth, **kwargs)

    def _post(self, **kwargs) -> requests.Response:
        return requests.post(auth=self.auth, **kwargs)

    @retry
    def post(self, **kwargs) -> requests.Response:
        return self._post(**kwargs)

    def _put(self, **kwargs) -> requests.Response:
        return requests.put(auth=self.auth, **kwargs)

    @retry
    def put(self, **kwargs) -> requests.Response:
        return self._put(**kwargs)

    def _delete(self, **kwargs) -> requests.Response:
        return requests.delete(auth=self.auth, **kwargs)

    def delete(self, **kwargs) -> requests.Response:
        return self._delete(**kwargs)

    def set_data_path(self, host: str, path: str):
        logger.info('Configuring data path on {}'.format(host))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'path': path,
        }
        self.post(url=api, data=data)

    def set_index_path(self, host: str, path: str):
        logger.info('Configuring index path on {}'.format(host))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'index_path': path,
        }
        self.post(url=api, data=data)

    def set_analytics_paths(self, host: str, paths: List[str]):
        logger.info('Configuring analytics path on {}: {}'.format(host, paths))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'cbas_path': paths,
        }
        self.post(url=api, data=data)

    def set_auth(self, host: str):
        logger.info('Configuring cluster authentication: {}'.format(host))

        api = 'http://{}:8091/settings/web'.format(host)
        data = {
            'username': self.rest_username, 'password': self.rest_password,
            'port': 'SAME'
        }
        self.post(url=api, data=data)

    def rename(self, host: str):
        logger.info('Changing server name: {}'.format(host))

        api = 'http://{}:8091/node/controller/rename'.format(host)
        data = {'hostname': host}

        self.post(url=api, data=data)

    def set_mem_quota(self, host: str, mem_quota: str):
        logger.info('Configuring data RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'memoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_index_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring index RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'indexMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_fts_index_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring FTS RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'ftsMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_analytics_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring Analytics RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'cbasMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_eventing_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring eventing RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'eventingMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_query_settings(self, host: str, override_settings: dict):
        api = 'http://{}:8093/admin/settings'.format(host)

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error('Cannot change query setting {} to {}, setting invalid'
                             .format(override, value))
                continue
            settings[override] = value
            logger.info('Changing {} to {}'.format(override, value))
        self.post(url=api, data=json.dumps(settings))

    def get_query_settings(self, host: str):
        api = 'http://{}:8093/admin/settings'.format(host)

        return self.get(url=api).json()

    def set_index_settings(self, host: str, settings: dict):
        api = 'http://{}:9102/settings'.format(host)

        curr_settings = self.get_index_settings(host)
        for option, value in settings.items():
            if option in curr_settings:
                logger.info('Changing {} to {}'.format(option, value))
                self.post(url=api, data=json.dumps({option: value}))
            else:
                logger.warn('Skipping unknown option: {}'.format(option))

    def get_index_settings(self, host: str) -> dict:
        api = 'http://{}:9102/settings?internal=ok'.format(host)

        return self.get(url=api).json()

    def get_gsi_stats(self, host: str) -> dict:
        api = 'http://{}:9102/stats'.format(host)

        return self.get(url=api).json()

    def create_index(self, host: str, bucket: str, name: str, field: str,
                     storage: str = 'memdb'):
        api = 'http://{}:9102/createIndex'.format(host)
        data = {
            'index': {
                'bucket': bucket,
                'using': storage,
                'name': name,
                'secExprs': ['`{}`'.format(field)],
                'exprType': 'N1QL',
                'isPrimary': False,
                'where': '',
                'deferred': False,
                'partitionKey': '',
                'partitionScheme': 'SINGLE',
            },
            'type': 'create',
            'version': 1,
        }
        logger.info('Creating index {}'.format(pretty_dict(data)))
        self.post(url=api, data=json.dumps(data))

    def set_services(self, host: str, services: str):
        logger.info('Configuring services on {}: {}'.format(host, services))

        api = 'http://{}:8091/node/controller/setupServices'.format(host)
        data = {'services': services}
        self.post(url=api, data=data)

    def add_node(self, host: str, new_host: str, services: str = None):
        logger.info('Adding new node: {}'.format(new_host))

        api = 'http://{}:8091/controller/addNode'.format(host)
        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services,
        }
        self.post(url=api, data=data)

    def rebalance(self, host: str, known_nodes: List[str],
                  ejected_nodes: List[str]):
        logger.info('Starting rebalance')

        api = 'http://{}:8091/controller/rebalance'.format(host)
        known_nodes = ','.join(map(self.get_otp_node_name, known_nodes))
        ejected_nodes = ','.join(map(self.get_otp_node_name, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=api, data=data)

    def increase_bucket_limit(self, host: str, num_buckets: int):
        logger.info('increasing bucket limit to {}'.format(num_buckets))

        api = 'http://{}:8091/internalSettings'.format(host)
        data = {
            'maxBucketCount': num_buckets
        }
        self.post(url=api, data=data)

    def get_counters(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default'.format(host)
        return self.get(url=api).json()['counters']

    def is_not_balanced(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('rebalance_start') - counters.get('rebalance_success')

    def get_failover_counter(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('failover_node')

    def get_tasks(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default/tasks'.format(host)
        return self.get(url=api).json()

    def get_task_status(self, host: str, task_type: str) -> [bool, float]:
        for task in self.get_tasks(host):
            if task['type'] == task_type:
                is_running = task['status'] == 'running'
                progress = task.get('progress')
                return is_running, progress
        return False, 0

    def get_xdcrlink_status(self, host: str, task_type: str, uuid: str) -> [bool, float]:
        for task in self.get_tasks(host):
            if task['type'] == task_type and uuid in task['target']:
                is_running = task['status'] == 'running'
                progress = task.get('progress')
                return is_running, progress
        return False, 0

    def get_xdcr_replication_id(self, host: str):
        replication_id = ''
        for task in self.get_tasks(host):
            if 'settingsURI' in task:
                replication_id = task['settingsURI']
        return replication_id.split('/')[-1]

    def delete_bucket(self, host: str, name: str):
        logger.info('Deleting new bucket: {}'.format(name))
        api = 'http://{host}:8091/pools/default/buckets/{bucket}'.format(host=host, bucket=name)
        self.delete(url=api)

    def create_bucket(self,
                      host: str,
                      name: str,
                      password: str,
                      ram_quota: int,
                      replica_number: int,
                      replica_index: int,
                      eviction_policy: str,
                      bucket_type: str,
                      backend_storage: str = None,
                      conflict_resolution_type: str = None,
                      compression_mode: str = None):
        logger.info('Adding new bucket: {}'.format(name))

        api = 'http://{}:8091/pools/default/buckets'.format(host)

        data = {
            'name': name,
            'bucketType': bucket_type,
            'ramQuotaMB': ram_quota,
            'evictionPolicy': eviction_policy,
            'flushEnabled': 1,
            'replicaNumber': replica_number,
            'authType': 'sasl',
            'saslPassword': password,
        }

        if bucket_type == BucketSettings.BUCKET_TYPE:
            data['replicaIndex'] = replica_index

        if conflict_resolution_type:
            data['conflictResolutionType'] = conflict_resolution_type

        if compression_mode:
            data['compressionMode'] = compression_mode

        if backend_storage:
            data['storageBackend'] = backend_storage

        logger.info('Bucket configuration: {}'.format(pretty_dict(data)))

        self.post(url=api, data=data)

    def flush_bucket(self, host: str, bucket: str):
        logger.info('Flushing bucket: {}'.format(bucket))

        api = 'http://{}:8091/pools/default/buckets/{}/controller/doFlush'.format(host, bucket)
        self.post(url=api)

    def configure_auto_compaction(self, host, settings):
        logger.info('Applying auto-compaction settings: {}'.format(settings))

        api = 'http://{}:8091/controller/setAutoCompaction'.format(host)
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower()
        }
        self.post(url=api, data=data)

    def get_auto_compaction_settings(self, host: str) -> dict:
        api = 'http://{}:8091/settings/autoCompaction'.format(host)
        return self.get(url=api).json()

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        api = 'http://{}:8091/pools/default/buckets/{}/stats'.format(host,
                                                                     bucket)
        return self.get(url=api).json()

    def get_dcp_replication_items(self, host: str, bucket: str) -> dict:
        api = 'http://{}:8091/pools/default/stats/range/kv_dcp_items_remaining?bucket={}&' \
              'connection_type=replication&aggregationFunction=sum'.format(host, bucket)
        return self.get(url=api).json()

    def get_xdcr_stats(self, host: str, bucket: str) -> dict:
        api = 'http://{}:8091/pools/default/buckets/@xdcr-{}/stats'.format(host,
                                                                           bucket)
        return self.get(url=api).json()

    def add_remote_cluster(self,
                           local_host: str,
                           remote_host: str,
                           name: str,
                           secure_type: str,
                           certificate: str):
        logger.info('Adding a remote cluster: {}'.format(remote_host))

        api = 'http://{}:8091/pools/default/remoteClusters'.format(local_host)
        payload = {
            'name': name,
            'hostname': remote_host,
            'username': self.rest_username,
            'password': self.rest_password,
        }
        if secure_type:
            payload['secureType'] = secure_type
        if certificate:
            payload['demandEncryption'] = 1
            payload['certificate'] = certificate

        self.post(url=api, data=payload)

    def get_remote_clusters(self, host: str) -> List[Dict]:
        logger.info('Getting remote clusters')

        api = 'http://{}:8091/pools/default/remoteClusters'.format(host)
        return self.get(url=api).json()

    def create_replication(self, host: str, params: dict):
        logger.info('Starting replication with parameters {}'.format(params))

        api = 'http://{}:8091/controller/createReplication'.format(host)
        self.post(url=api, data=params)

    def edit_replication(self, host: str, params: dict, replicationid: str):
        logger.info('Editing replication {} with parameters {}'.format(replicationid, params))

        api = 'http://{}:8091/settings/replications/{}'.format(host, replicationid)
        self.post(url=api, data=params)

    def trigger_bucket_compaction(self, host: str, bucket: str):
        logger.info('Triggering bucket {} compaction'.format(bucket))

        api = 'http://{}:8091/pools/default/buckets/{}/controller/compactBucket'\
            .format(host, bucket)
        self.post(url=api)

    def trigger_index_compaction(self, host: str, bucket: str, ddoc: str):
        logger.info('Triggering ddoc {} compaction, bucket {}'.format(
            ddoc, bucket
        ))

        api = 'http://{}:8091/pools/default/buckets/{}/ddocs/_design%2F{}/controller/compactView'\
            .format(host, bucket, ddoc)
        self.post(url=api)

    def create_ddoc(self, host: str, bucket: str, ddoc_name: str, ddoc: dict):
        logger.info('Creating new ddoc {}, bucket {}'.format(
            ddoc_name, bucket
        ))

        api = 'http://{}:8091/couchBase/{}/_design/{}'.format(
            host, bucket, ddoc_name)
        data = json.dumps(ddoc)
        headers = {'Content-type': 'application/json'}
        self.put(url=api, data=data, headers=headers)

    def query_view(self, host: str, bucket: str, ddoc_name: str,
                   view_name: str, params: dict):
        logger.info('Querying view: {}/_design/{}/_view/{}'.format(
            bucket, ddoc_name, view_name
        ))

        api = 'http://{}:8091/couchBase/{}/_design/{}/_view/{}'.format(
            host, bucket, ddoc_name, view_name)
        self.get(url=api, params=params)

    def get_version(self, host: str) -> str:
        logger.info('Getting Couchbase Server version')

        api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return r['implementationVersion'] \
            .replace('-rel-enterprise', '') \
            .replace('-enterprise', '') \
            .replace('-community', '')

    def supports_rbac(self, host: str) -> bool:
        """Return true if the cluster supports RBAC."""
        rbac_url = 'http://{}:8091/settings/rbac/roles'.format(host)
        r = requests.get(auth=self.auth, url=rbac_url)
        return r.status_code == requests.codes.ok

    def is_community(self, host: str) -> bool:
        logger.info('Getting Couchbase Server edition')

        api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return 'community' in r['implementationVersion']

    def get_memcached_port(self, host: str) -> int:
        logger.info('Getting memcached port from {}'.format(host))

        api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['ports']['direct']

    def get_otp_node_name(self, host: str) -> str:
        logger.info('Getting OTP node name from {}'.format(host))

        api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['otpNode']

    def set_internal_settings(self, host: str, data: dict):
        logger.info('Updating internal settings: {}'.format(data))

        api = 'http://{}:8091/internalSettings'.format(host)
        self.post(url=api, data=data)

    def set_xdcr_cluster_settings(self, host: str, data: dict):
        logger.info('Updating xdcr cluster settings: {}'.format(data))

        api = 'http://{}:8091/settings/replications'.format(host)
        self.post(url=api, data=data)

    def run_diag_eval(self, host: str, cmd: str):
        api = 'http://{}:8091/diag/eval'.format(host)
        self.post(url=api, data=cmd)

    def set_auto_failover(self, host: str, enabled: str,
                          failover_min: int, failover_max: int):
        logger.info('Enabling auto-failover with the minimum timeout')

        api = 'http://{}:8091/settings/autoFailover'.format(host)

        for timeout in failover_min, failover_max:
            data = {'enabled': enabled,
                    'timeout': timeout,
                    'failoverOnDataDiskIssues[enabled]': enabled,
                    'failoverOnDataDiskIssues[timePeriod]': 10
                    }
            r = self._post(url=api, data=data)
            if r.status_code == 200:
                break

    def get_certificate(self, host: str) -> str:
        logger.info('Getting remote certificate')

        api = 'http://{}:8091/pools/default/certificate'.format(host)
        return self.get(url=api).text

    def fail_over(self, host: str, node: str):
        logger.info('Failing over node: {}'.format(node))

        api = 'http://{}:8091/controller/failOver'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def graceful_fail_over(self, host: str, node: str):
        logger.info('Gracefully failing over node: {}'.format(node))

        api = 'http://{}:8091/controller/startGracefulFailover'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def add_back(self, host: str, node: str):
        logger.info('Adding node back: {}'.format(node))

        api = 'http://{}:8091/controller/reAddNode'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def set_delta_recovery_type(self, host: str, node: str):
        logger.info('Enabling delta recovery: {}'.format(node))

        api = 'http://{}:8091/controller/setRecoveryType'.format(host)
        data = {
            'otpNode': self.get_otp_node_name(node),
            'recoveryType': 'delta'  # alt: full
        }
        self.post(url=api, data=data)

    def node_statuses(self, host: str) -> dict:
        api = 'http://{}:8091/nodeStatuses'.format(host)
        data = self.get(url=api).json()
        return {node: info['status'] for node, info in data.items()}

    def node_statuses_v2(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default'.format(host)
        data = self.get(url=api).json()
        return {node['hostname']: node['status'] for node in data['nodes']}

    def get_node_stats(self, host: str, bucket: str) -> Iterator:
        api = 'http://{}:8091/pools/default/buckets/{}/nodes'.format(host,
                                                                     bucket)
        data = self.get(url=api).json()
        for server in data['servers']:
            api = 'http://{}:8091{}'.format(host, server['stats']['uri'])
            data = self.get(url=api).json()
            yield data['hostname'], data['op']['samples']

    def get_vbmap(self, host: str, bucket: str) -> dict:
        logger.info('Reading vbucket map: {}/{}'.format(host, bucket))
        api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return data['vBucketServerMap']['vBucketMap']

    def get_server_list(self, host: str, bucket: str) -> List[str]:
        api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return [server.split(':')[0]
                for server in data['vBucketServerMap']['serverList']]

    def get_bucket_info(self, host: str, bucket: str) -> List[str]:
        api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        return self.get(url=api).json()

    def exec_n1ql_statement(self, host: str, statement: str) -> dict:
        api = 'http://{}:8093/query/service'.format(host)
        data = {
            'statement': statement,
        }

        response = self.post(url=api, data=data)
        return response.json()

    def explain_n1ql_statement(self, host: str, statement: str):
        statement = 'EXPLAIN {}'.format(statement)
        return self.exec_n1ql_statement(host, statement)

    def get_query_stats(self, host: str) -> dict:
        logger.info('Getting query engine stats')

        api = 'http://{}:8093/admin/stats'.format(host)

        response = self.get(url=api)
        return response.json()

    def delete_fts_index(self, host: str, index: str):
        logger.info('Deleting FTS index: {}'.format(index))

        api = 'http://{}:8094/api/index/{}'.format(host, index)

        self.delete(url=api)

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))

        api = 'http://{}:8094/api/index/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)

        self.put(url=api, data=data, headers=headers)

    def get_fts_doc_count(self, host: str, index: str) -> int:
        api = 'http://{}:8094/api/index/{}/count'.format(host, index)

        response = self.get(url=api).json()
        return response['count']

    def get_fts_stats(self, host: str) -> dict:
        api = 'http://{}:8094/api/nsstats'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_elastic_stats(self, host: str) -> dict:
        api = "http://{}:9200/_stats".format(host)
        response = self.get(url=api)
        return response.json()

    def delete_elastic_index(self, host: str, index: str):
        logger.info('Deleting Elasticsearch index: {}'.format(index))

        api = 'http://{}:9200/{}'.format(host, index)

        self.delete(url=api)

    def create_elastic_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new Elasticsearch index: {}'.format(index))

        api = 'http://{}:9200/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)

        self.put(url=api, data=data, headers=headers)

    def get_elastic_doc_count(self, host: str, index: str) -> int:
        api = "http://{}:9200/{}/_count".format(host, index)
        response = self.get(url=api).json()
        return response['count']

    def get_index_status(self, host: str) -> dict:
        api = 'http://{}:9102/getIndexStatus'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_index_stats(self, hosts: List[str]) -> dict:
        api = 'http://{}:9102/stats'
        data = {}
        for host in hosts:
            host_data = self.get(url=api.format(host))
            data.update(host_data.json())
        return data

    def get_index_num_connections(self, host: str) -> int:
        api = 'http://{}:9102/stats'.format(host)
        response = self.get(url=api).json()
        return response['num_connections']

    def get_index_storage_stats(self, host: str) -> str:
        api = 'http://{}:9102/stats/storage'.format(host)
        return self.get(url=api)

    def get_index_storage_stats_mm(self, host: str) -> str:
        api = 'http://{}:9102/stats/storage/mm'.format(host)
        return self.get(url=api).text

    def get_audit_settings(self, host: str) -> dict:
        logger.info('Getting current audit settings')

        api = 'http://{}:8091/settings/audit'.format(host)
        return self.get(url=api).json()

    def enable_audit(self, host: str, disabled: List[str]):
        logger.info('Enabling audit')

        api = 'http://{}:8091/settings/audit'.format(host)
        data = {
            'auditdEnabled': 'true',
        }
        if disabled:
            data['disabled'] = ','.join(disabled)
        self.post(url=api, data=data)

    def get_rbac_roles(self, host: str) -> List[dict]:
        logger.info('Getting the existing RBAC roles')

        api = 'http://{}:8091/settings/rbac/roles'.format(host)

        return self.get(url=api).json()

    def delete_rbac_user(self, host: str, bucket: str):
        logger.info('Deleting an RBAC user: {}'.format(bucket))
        for domain in 'local', 'builtin':
            api = 'http://{}:8091/settings/rbac/users/{}/{}'.format(host,
                                                                    domain,
                                                                    bucket)
            r = self._delete(url=api)
            if r.status_code == 200:
                break

    def add_rbac_user(self, host: str, user: str, password: str,
                      roles: List[str]):
        logger.info('Adding an RBAC user: {}, roles: {}'.format(user,
                                                                roles))
        data = {
            'password': password,
            'roles': ','.join(roles),
        }

        for domain in 'local', 'builtin':
            api = 'http://{}:8091/settings/rbac/users/{}/{}'.format(host,
                                                                    domain,
                                                                    user)
            r = self._put(url=api, data=data)
            if r.status_code == 200:
                break

    def analytics_node_active(self, host: str) -> bool:
        logger.info('Checking if analytics node is active: {}'.format(host))

        api = 'http://{}:{}/analytics/cluster'.format(host, ANALYTICS_PORT)

        status = self.get(url=api).json()
        return status["state"] == "ACTIVE"

    def exec_analytics_statement(self, analytics_node: str,
                                 statement: str) -> requests.Response:
        api = 'http://{}:{}/analytics/service'.format(analytics_node,
                                                      ANALYTICS_PORT)
        data = {
            'statement': statement
        }
        return self.post(url=api, data=data)

    def get_analytics_stats(self, analytics_node: str) -> dict:
        api = 'http://{}:9110/analytics/node/stats'.format(analytics_node)
        return self.get(url=api).json()

    def get_pending_mutations(self, analytics_node: str) -> dict:
        api = 'http://{}:8095/analytics/node/agg/stats/remaining'.format(analytics_node)
        return self.get(url=api).json()

    def set_analytics_logging_level(self, analytics_node: str, log_level: str):
        logger.info('Setting log level \"{}\" for analytics'.format(log_level))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'logLevel': log_level
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_page_size(self, analytics_node: str, page_size: str):
        logger.info('Setting buffer cache page size \"{}\" for analytics'.format(page_size))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageBuffercachePagesize': page_size
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_storage_compression_block(self, analytics_node: str,
                                                storage_compression_block: str):
        logger.info('Setting storage compression block \"{}\" for analytics'
                    .format(storage_compression_block))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageCompressionBlock': storage_compression_block
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_max_active_writable_datasets(
            self,
            analytics_node: str,
            max_writable: int):
        logger.info('Setting max active writable datasets \"{}\" for analytics'
                    .format(str(max_writable)))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageMaxActiveWritableDatasets': str(max_writable)
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def restart_analytics_cluster(self, analytics_node: str):
        logger.info('Restarting analytics cluster')
        api = 'http://{}:{}/analytics/cluster/restart'.format(analytics_node, ANALYTICS_PORT)
        r = self.post(url=api)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def validate_analytics_logging_level(self, analytics_node: str, log_level: str):
        logger.info('Checking that analytics log level is set to {}'.format(log_level))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        if "logLevel" in response:
            return response["logLevel"] == log_level
        return False

    def validate_analytics_setting(self, analytics_node: str, setting: str, value: str):
        logger.info('Checking that analytics {} is set to {}'.format(setting, value))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        assert(str(response[setting]) == str(value))

    def get_analytics_service_config(self, analytics_node: str):
        logger.info('Grabbing analytics service config')
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        return response

    def deploy_function(self, node: str, func: dict, name: str):
        logger.info('Deploying function on node {}: {}'.format(node, pretty_dict(func)))
        api = 'http://{}:8096/api/v1/functions/{}'.format(node, name)
        self.post(url=api, data=json.dumps(func))

    def change_function_settings(self, node: str, func: dict, name: str):
        logger.info('Changing function settings on on node {}: {}'.format(node,
                                                                          pretty_dict(func)))
        api = 'http://{}:8096/api/v1/functions/{}/settings/'.format(node, name)
        self.post(url=api, data=func)

    def get_num_events_processed(self, event: str, node: str, name: str):
        logger.info('get stats on node {} for {}'.format(node, name))

        data = {}
        all_stats = self.get_eventing_stats(node=node)
        for stat in all_stats:
            if name == stat["function_name"]:
                data = stat["event_processing_stats"]
                break

        logger.info(data)
        if event == "ALL":
            return data
        if event in data:
            return data[event]

        return 0

    def get_apps_with_status(self, node: str, status: str):
        logger.info('get apps with status {} on node {}'.format(status, node))

        api = 'http://{}:{}//api/v1/status'.format(node, EVENTING_PORT)
        data = self.get(url=api).json()
        apps = []
        for app in data["apps"]:
            if app["composite_status"] == status:
                apps.append(app["name"])
        return apps

    def get_eventing_stats(self, node: str, full_stats: bool = False) -> dict:
        logger.info('get eventing stats on node {}'.format(node))

        api = 'http://{}:{}/api/v1/stats'.format(node, EVENTING_PORT)
        if full_stats:
            api += "?type=full"

        return self.get(url=api).json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> List[str]:
        active_nodes = self.node_statuses(master_node)
        active_nodes_by_role = []
        for node in self.cluster_spec.servers_by_role(role):
            if node + ":8091" in active_nodes:
                active_nodes_by_role.append(node)
        return active_nodes_by_role

    def fts_set_node_level_parameters(self, parameter: dict, host: str):
        logger.info("Adding in the parameter {} ".format(parameter))
        api = "http://{}:8094/api/managerOptions".format(host)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(parameter, ensure_ascii=False)
        self.put(url=api, data=data, headers=headers)

    def upload_cluster_certificate(self, node: str):
        logger.info("Uploading cluster certificate to {}".format(node))
        api = 'http://{}:8091/controller/uploadClusterCA'.format(node)
        data = open('./certificates/inbox/ca.pem', 'rb').read()
        self.post(url=api, data=data)

    def reload_cluster_certificate(self, node: str):
        logger.info("Reloading certificate on {}".format(node))
        api = 'http://{}:8091/node/controller/reloadCertificate'.format(node)
        self.post(url=api)

    def enable_certificate_auth(self, node: str):
        logger.info("Enabling certificate-based client auth on {}".format(node))
        api = 'http://{}:8091/settings/clientCertAuth'.format(node)
        data = open('./certificates/inbox/config.json', 'rb').read()
        self.post(url=api, data=data)

    def get_minimum_tls_version(self, node: str):
        logger.info("Getting TLS version of {}".format(node))
        api = 'http://{}:8091/settings/security'.format(node)
        return self.get(url=api).json()['tlsMinVersion']

    def set_num_threads(self, node: str, thread_type: str, thread: int):
        logger.info('Setting {} to {}'.format(thread_type, thread))
        api = 'http://{}:8091/pools/default/settings/memcached/global'.format(node)
        data = {
            thread_type: thread
        }
        self.post(url=api, data=data)

    def get_cipher_suite(self, node: str):
        logger.info("Getting cipher suites of {}".format(node))
        api = 'http://{}:8091/settings/security'.format(node)
        return self.get(url=api).json()['cipherSuites']

    def set_cipher_suite(self, node: str, cipher_list: list):
        logger.info("Setting cipher list of {}".format(cipher_list))
        api = 'http://{}:8091/settings/security'.format(node)
        data = {
            'cipherSuites': json.dumps(cipher_list)
        }
        self.post(url=api, data=data)

    def set_minimum_tls_version(self, node: str, tls_version: str):
        logger.info("Setting minimum TLS version of {}".format(tls_version))
        api = 'http://{}:8091/settings/security'.format(node)
        data = {
            'tlsMinVersion': tls_version
        }
        self.post(url=api, data=data)

    def create_scope(self, host, bucket, scope):
        logger.info("Creating scope {}:{}".format(bucket, scope))
        api = 'http://{}:8091/pools/default/buckets/{}/collections'.format(host, bucket)
        data = {
            'name': scope
        }
        self.post(url=api, data=data)

    def create_collection(self, host, bucket, scope, collection):
        logger.info("Creating collection {}:{}.{}".format(bucket, scope, collection))
        api = 'http://{}:8091/pools/default/buckets/{}/collections/{}'.format(host, bucket, scope)
        data = {
            'name': collection
        }
        self.post(url=api, data=data)

    def delete_collection(self, host, bucket, scope, collection):
        logger.info("Dropping collection {}:{}.{}".format(bucket, scope, collection))
        api = 'http://{}:8091/pools/default/buckets/{}/collections/{}/{}'\
            .format(host, bucket, scope, collection)
        self.delete(url=api)
