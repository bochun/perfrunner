import re
from typing import List

from logger import logger
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import maybe_atoi, pretty_dict
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig


class ClusterManager:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig,
                 verbose: bool = False):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.dynamic_infra = self.cluster_spec.dynamic_infrastructure
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.monitor = Monitor(cluster_spec, test_config, verbose)
        self.memcached = MemcachedHelper(test_config)
        self.master_node = next(self.cluster_spec.masters)

        self.initial_nodes = test_config.cluster.initial_nodes
        self.build = self.rest.get_version(self.master_node)

    def is_compatible(self, min_release: str) -> bool:
        for master in self.cluster_spec.masters:
            version = self.rest.get_version(master)
            return version >= min_release

    def set_data_path(self):
        if self.dynamic_infra:
            return
        for server in self.cluster_spec.servers:
            self.remote.change_owner(server, self.cluster_spec.data_path)
            self.rest.set_data_path(server, self.cluster_spec.data_path)

    def set_index_path(self):
        if self.dynamic_infra:
            return
        for server in self.cluster_spec.servers:
            self.remote.change_owner(server, self.cluster_spec.index_path)
            self.rest.set_index_path(server, self.cluster_spec.index_path)

    def set_analytics_path(self):
        if self.dynamic_infra:
            return
        paths = []
        for path in self.cluster_spec.analytics_paths:
            for i in range(self.test_config.analytics_settings.num_io_devices):
                io_device = '{}/dev{}'.format(path, i)
                paths.append(io_device)
        for server in self.cluster_spec.servers_by_role('cbas'):
            for path in self.cluster_spec.analytics_paths:
                self.remote.change_owner(server, path)
            self.rest.set_analytics_paths(server, paths)

    def rename(self):
        if self.dynamic_infra:
            return
        else:
            for server in self.cluster_spec.servers:
                self.rest.rename(server)

    def set_auth(self):
        if self.dynamic_infra:
            return
        else:
            for server in self.cluster_spec.servers:
                self.rest.set_auth(server)

    def set_mem_quotas(self):
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            cluster['spec']['cluster']['dataServiceMemoryQuota'] = \
                '{}Mi'.format(self.test_config.cluster.mem_quota)
            cluster['spec']['cluster']['indexServiceMemoryQuota'] = \
                '{}Mi'.format(self.test_config.cluster.index_mem_quota)
            if self.test_config.cluster.fts_index_mem_quota:
                cluster['spec']['cluster']['searchServiceMemoryQuota'] = \
                    '{}Mi'.format(self.test_config.cluster.fts_index_mem_quota)
            if self.test_config.cluster.analytics_mem_quota:
                cluster['spec']['cluster']['analyticsServiceMemoryQuota'] = \
                    '{}Mi'.format(self.test_config.cluster.analytics_mem_quota)
            if self.test_config.cluster.eventing_mem_quota:
                cluster['spec']['cluster']['eventingServiceMemoryQuota'] = \
                    '{}Mi'.format(self.test_config.cluster.eventing_mem_quota)
            self.remote.update_cluster_config(cluster)
        else:
            for master in self.cluster_spec.masters:
                self.rest.set_mem_quota(master,
                                        self.test_config.cluster.mem_quota)
                self.rest.set_index_mem_quota(master,
                                              self.test_config.cluster.index_mem_quota)
                if self.test_config.cluster.fts_index_mem_quota:
                    self.rest.set_fts_index_mem_quota(master,
                                                      self.test_config.cluster.fts_index_mem_quota)
                if self.test_config.cluster.analytics_mem_quota:
                    self.rest.set_analytics_mem_quota(master,
                                                      self.test_config.cluster.analytics_mem_quota)
                if self.test_config.cluster.eventing_mem_quota:
                    self.rest.set_eventing_mem_quota(master,
                                                     self.test_config.cluster.eventing_mem_quota)

    def set_query_settings(self):
        logger.info('Setting query settings')
        if self.dynamic_infra:
            return
        query_nodes = self.cluster_spec.servers_by_role('n1ql')
        if query_nodes:
            settings = self.test_config.n1ql_settings.cbq_settings
            if settings:
                self.rest.set_query_settings(query_nodes[0], settings)
            settings = self.rest.get_query_settings(query_nodes[0])
            settings = pretty_dict(settings)
            logger.info('Query settings: {}'.format(settings))

    def set_index_settings(self):
        logger.info('Setting index settings')
        index_nodes = self.cluster_spec.servers_by_role('index')
        if index_nodes:
            settings = self.test_config.gsi_settings.settings
            if settings:
                if self.dynamic_infra:
                    cluster = self.remote.get_cluster()
                    cluster['spec']['cluster']['indexStorageSetting'] = \
                        settings['indexer.settings.storage_mode']
                    self.remote.update_cluster_config(cluster, timeout=300, reboot=True)
                    logger.info('Index settings: {}'.format(settings))
                else:
                    self.rest.set_index_settings(index_nodes[0], settings)
                    settings = self.rest.get_index_settings(index_nodes[0])
                    settings = pretty_dict(settings)
                    logger.info('Index settings: {}'.format(settings))

    def set_services(self):
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            server_types = dict()
            server_roles = self.cluster_spec.roles
            for server, role in server_roles.items():
                role = role\
                    .replace('kv', 'data')\
                    .replace('n1ql', 'query')
                server_type_count = server_types.get(role, 0)
                server_types[role] = server_type_count + 1

            istio = 'false'
            if self.cluster_spec.istio_enabled(cluster_name='k8s_cluster_1'):
                istio = 'true'

            cluster_servers = []
            operator_version = self.remote.get_operator_version()
            operator_major = int(operator_version.split(".")[0])
            operator_minor = int(operator_version.split(".")[1])
            for server_role, server_role_count in server_types.items():
                node_selector = {
                    '{}_enabled'.format(service
                                        .replace('data', 'kv')
                                        .replace('query', 'n1ql')): 'true'
                    for service in server_role.split(",")
                }
                node_selector['NodeRoles'] = 'couchbase1'
                spec = {
                    'imagePullSecrets': [{'name': 'regcred'}],
                    'nodeSelector': node_selector,
                }
                if (operator_major, operator_minor) <= (2, 1):
                    spec['containers'] = []
                pod_def =\
                    {
                        'spec': spec,
                        'metadata':
                            {
                                'annotations': {'sidecar.istio.io/inject': istio}
                            }
                    }
                server_def = \
                    {
                        'name': server_role.replace(",", "-"),
                        'services': server_role.split(","),
                        'pod': pod_def,
                        'size': server_role_count,
                        'volumeMounts': {'default': 'couchbase_kv'}
                    }
                cluster_servers.append(server_def)

            cluster['spec']['servers'] = cluster_servers
            self.remote.update_cluster_config(cluster, timeout=300, reboot=True)
        else:
            if not self.is_compatible(min_release='4.0.0'):
                return

            for master in self.cluster_spec.masters:
                roles = self.cluster_spec.roles[master]
                self.rest.set_services(master, roles)

    def add_nodes(self):
        if self.dynamic_infra:
            return
        for (_, servers), initial_nodes \
                in zip(self.cluster_spec.clusters, self.initial_nodes):

                if initial_nodes < 2:  # Single-node cluster
                    continue

                master = servers[0]
                for node in servers[1:initial_nodes]:
                    roles = self.cluster_spec.roles[node]
                    self.rest.add_node(master, node, roles)

    def rebalance(self):
        if self.dynamic_infra:
            return
        for (_, servers), initial_nodes \
                in zip(self.cluster_spec.clusters, self.initial_nodes):
            master = servers[0]
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)
        self.wait_until_healthy()

    def increase_bucket_limit(self, num_buckets: int):
        if self.dynamic_infra:
            return
        for master in self.cluster_spec.masters:
            self.rest.increase_bucket_limit(master, num_buckets)

    def flush_buckets(self):
        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.flush_bucket(host=master,
                                       bucket=bucket_name)

    def delete_buckets(self):
        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.delete_bucket(host=master,
                                        name=bucket_name)

    def create_buckets(self):
        mem_quota = self.test_config.cluster.mem_quota
        if self.test_config.cluster.num_buckets > 7:
            self.increase_bucket_limit(self.test_config.cluster.num_buckets + 3)

        if self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            mem_quota -= (self.test_config.cluster.eventing_metadata_bucket_mem_quota +
                          self.test_config.cluster.eventing_bucket_mem_quota)
        per_bucket_quota = mem_quota // self.test_config.cluster.num_buckets
        if self.dynamic_infra:
            self.remote.delete_all_buckets()
            for bucket_name in self.test_config.buckets:
                self.remote.create_bucket(bucket_name, per_bucket_quota, self.test_config.bucket)
        else:
            if self.test_config.bucket.backend_storage == 'magma':
                self.enable_developer_preview()
            for master in self.cluster_spec.masters:
                for bucket_name in self.test_config.buckets:
                    self.rest.create_bucket(
                        host=master,
                        name=bucket_name,
                        ram_quota=per_bucket_quota,
                        password=self.test_config.bucket.password,
                        replica_number=self.test_config.bucket.replica_number,
                        replica_index=self.test_config.bucket.replica_index,
                        eviction_policy=self.test_config.bucket.eviction_policy,
                        bucket_type=self.test_config.bucket.bucket_type,
                        backend_storage=self.test_config.bucket.backend_storage,
                        conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                        compression_mode=self.test_config.bucket.compression_mode,
                    )

    def create_collections(self):
        if self.dynamic_infra:
            return
        collection_map = self.test_config.collection.collection_map
        for master in self.cluster_spec.masters:
            if collection_map is not None:
                if self.test_config.collection.use_bulk_api:
                    for bucket in collection_map.keys():
                        create_scopes = []
                        for scope in collection_map[bucket]:
                            scope_collections = []
                            for collection in collection_map[bucket][scope]:
                                scope_collections.append({"name": collection})
                            create_scopes.append({"name": scope, "collections": scope_collections})
                        self.rest.set_collection_map(master, bucket, {"scopes": create_scopes})
                else:
                    for bucket in collection_map.keys():
                        delete_default = True
                        for scope in collection_map[bucket]:
                            if scope == '_default':
                                for collection in collection_map[bucket][scope]:
                                    if collection == "_default":
                                        delete_default = False
                        if delete_default:
                            self.rest.delete_collection(master, bucket, '_default', '_default')

                    for bucket in collection_map.keys():
                        for scope in collection_map[bucket]:
                            if scope != '_default':
                                self.rest.create_scope(master, bucket, scope)
                            for collection in collection_map[bucket][scope]:
                                if collection != '_default':
                                    self.rest.create_collection(master, bucket, scope, collection)

    def create_eventing_buckets(self):
        if not self.test_config.cluster.eventing_bucket_mem_quota:
            return
        if self.dynamic_infra:
            return
        per_bucket_quota = \
            self.test_config.cluster.eventing_bucket_mem_quota \
            // self.test_config.cluster.eventing_buckets

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.eventing_buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=per_bucket_quota,
                    password=self.test_config.bucket.password,
                    replica_number=self.test_config.bucket.replica_number,
                    replica_index=self.test_config.bucket.replica_index,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    bucket_type=self.test_config.bucket.bucket_type,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                )

    def create_eventing_metadata_bucket(self):
        if not self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            return
        if self.dynamic_infra:
            return
        for master in self.cluster_spec.masters:
            self.rest.create_bucket(
                host=master,
                name=self.test_config.cluster.EVENTING_METADATA_BUCKET_NAME,
                ram_quota=self.test_config.cluster.eventing_metadata_bucket_mem_quota,
                password=self.test_config.bucket.password,
                replica_number=self.test_config.bucket.replica_number,
                replica_index=self.test_config.bucket.replica_index,
                eviction_policy=self.test_config.bucket.EVICTION_POLICY,
                bucket_type=self.test_config.bucket.BUCKET_TYPE,
            )

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            db = int(compaction_settings.db_percentage)
            view = int(compaction_settings.view_percentage)
            para = bool(str(compaction_settings.parallel).lower())

            auto_compaction = cluster['spec']['cluster']\
                .get('autoCompaction',
                     {'databaseFragmentationThreshold': {'percent': 30},
                      'viewFragmentationThreshold': {'percent': 30},
                      'parallelCompaction': False})

            db_percent = auto_compaction.get(
                'databaseFragmentationThreshold', {'percent': 30})
            db_percent['percent'] = db
            auto_compaction['databaseFragmentationThreshold'] = db_percent

            views_percent = auto_compaction.get(
                'viewFragmentationThreshold', {'percent': 30})
            views_percent['percent'] = view
            auto_compaction['viewFragmentationThreshold'] = views_percent
            auto_compaction['parallelCompaction'] = para

            self.remote.update_cluster_config(cluster)
        else:
            for master in self.cluster_spec.masters:
                self.rest.configure_auto_compaction(master, compaction_settings)
                settings = self.rest.get_auto_compaction_settings(master)
                logger.info('Auto-compaction settings: {}'.format(pretty_dict(settings)))

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.cluster_spec.masters:
            for parameter, value in internal_settings.items():
                if self.dynamic_infra:
                    raise Exception('not supported for dynamic infrastructure yet')
                else:
                    self.rest.set_internal_settings(
                        master,
                        {parameter: maybe_atoi(value)})

    def configure_xdcr_settings(self):
        xdcr_cluster_settings = self.test_config.xdcr_cluster_settings
        if self.dynamic_infra:
            return
        for master in self.cluster_spec.masters:
            for parameter, value in xdcr_cluster_settings.items():
                self.rest.set_xdcr_cluster_settings(
                    master,
                    {parameter: maybe_atoi(value)})

    def tweak_memory(self):
        if self.dynamic_infra:
            return
        self.remote.reset_swap()
        self.remote.drop_caches()
        self.remote.set_swappiness()
        self.remote.disable_thp()

    def enable_n2n_encryption(self):
        if self.dynamic_infra:
            return
        if self.test_config.cluster.enable_n2n_encryption:
            for master in self.cluster_spec.masters:
                self.remote.enable_n2n_encryption(
                    master,
                    self.test_config.cluster.enable_n2n_encryption)

    def restart_with_alternative_num_vbuckets(self):
        num_vbuckets = self.test_config.cluster.num_vbuckets
        if num_vbuckets is not None:
            if self.dynamic_infra:
                raise Exception('not supported for dynamic infrastructure yet')
            else:
                self.remote.restart_with_alternative_num_vbuckets(num_vbuckets)

    def restart_with_alternative_bucket_options(self):
        """Apply custom buckets settings.

        Tune bucket settings (e.g., max_num_shards or max_num_auxio) using
        "/diag/eval" and restart the entire cluster.
        """
        if self.dynamic_infra:
            return
        if self.test_config.bucket_extras:
            self.remote.enable_nonlocal_diag_eval()

        cmd = 'ns_bucket:update_bucket_props("{}", ' \
              '[{{extra_config_string, "{}={}"}}]).'

        for option, value in self.test_config.bucket_extras.items():
            if re.search("^num_.*_threads$", option):
                self.rest.set_num_threads(self.master_node, option, value)
            else:
                logger.info('Changing {} to {}'.format(option, value))
                for master in self.cluster_spec.masters:
                    for bucket in self.test_config.buckets:
                        diag_eval = cmd.format(bucket, option, value)
                        self.rest.run_diag_eval(master, diag_eval)

        if self.test_config.bucket_extras:
            self.disable_auto_failover()
            self.remote.restart()
            self.wait_until_healthy()
            self.enable_auto_failover()

    def tune_logging(self):
        if self.dynamic_infra:
            return
        self.remote.tune_log_rotation()
        self.remote.restart()

    def enable_auto_failover(self):
        enabled = self.test_config.bucket.autofailover_enabled
        failover_min = self.test_config.bucket.failover_min
        failover_max = self.test_config.bucket.failover_max
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            cluster['spec']['cluster']['autoFailoverMaxCount'] = 1
            cluster['spec']['cluster']['autoFailoverServerGroup'] = bool(enabled)
            cluster['spec']['cluster']['autoFailoverOnDataDiskIssues'] = bool(enabled)
            cluster['spec']['cluster']['autoFailoverOnDataDiskIssuesTimePeriod'] = \
                '{}s'.format(10)
            cluster['spec']['cluster']['autoFailoverTimeout'] = \
                '{}s'.format(failover_max)
            self.remote.update_cluster_config(cluster)
        else:
            for master in self.cluster_spec.masters:
                self.rest.set_auto_failover(master, enabled, failover_min, failover_max)

    def disable_auto_failover(self):
        enabled = 'false'
        failover_min = self.test_config.bucket.failover_min
        failover_max = self.test_config.bucket.failover_max
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            cluster['spec']['cluster']['autoFailoverMaxCount'] = 1
            cluster['spec']['cluster']['autoFailoverServerGroup'] = bool(enabled)
            cluster['spec']['cluster']['autoFailoverOnDataDiskIssues'] = bool(enabled)
            cluster['spec']['cluster']['autoFailoverOnDataDiskIssuesTimePeriod'] = \
                '{}s'.format(10)
            cluster['spec']['cluster']['autoFailoverTimeout'] = \
                '{}s'.format(failover_max)
            self.remote.update_cluster_config(cluster)
        else:
            for master in self.cluster_spec.masters:
                self.rest.set_auto_failover(master, enabled, failover_min, failover_max)

    def wait_until_warmed_up(self):
        if self.test_config.bucket.bucket_type in ('ephemeral', 'memcached'):
            return
        if self.dynamic_infra:
            self.remote.wait_for_cluster_ready()
        else:
            for master in self.cluster_spec.masters:
                for bucket in self.test_config.buckets:
                    self.monitor.monitor_warmup(self.memcached, master, bucket)

    def wait_until_healthy(self):
        if self.dynamic_infra:
            self.remote.wait_for_cluster_ready()
        else:
            for master in self.cluster_spec.masters:
                self.monitor.monitor_node_health(master)

                for analytics_node in self.rest.get_active_nodes_by_role(master,
                                                                         'cbas'):
                    self.monitor.monitor_analytics_node_active(analytics_node)

    def gen_disabled_audit_events(self, master: str) -> List[str]:
        curr_settings = self.rest.get_audit_settings(master)
        curr_disabled = {str(event) for event in curr_settings['disabled']}
        disabled = curr_disabled - self.test_config.audit_settings.extra_events
        return list(disabled)

    def enable_audit(self):
        if self.dynamic_infra:
            return
        if not self.is_compatible(min_release='4.0.0') or \
                self.rest.is_community(self.master_node):
            return

        if not self.test_config.audit_settings.enabled:
            return

        for master in self.cluster_spec.masters:
            disabled = []
            if self.test_config.audit_settings.extra_events:
                disabled = self.gen_disabled_audit_events(master)
            self.rest.enable_audit(master, disabled)

    def generate_ce_roles(self) -> List[str]:
        return ['admin']

    def generate_ee_roles(self) -> List[str]:
        existing_roles = {r['role']
                          for r in self.rest.get_rbac_roles(self.master_node)}

        roles = []
        for role in (
                'bucket_admin',
                'data_dcp_reader',
                'data_monitoring',
                'data_reader_writer',
                'data_reader',
                'data_writer',
                'fts_admin',
                'fts_searcher',
                'query_delete',
                'query_insert',
                'query_select',
                'query_update',
                'views_admin',
        ):
            if role in existing_roles:
                roles.append(role + '[{bucket}]')

        return roles

    def delete_rbac_users(self):
        if not self.is_compatible(min_release='5.0'):
            return

        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.rest.delete_rbac_user(
                    host=master,
                    bucket=bucket
                )

    def add_rbac_users(self):
        if self.dynamic_infra:
            self.remote.create_from_file("cloud/operator/2/1/user-password-secret.yaml")
            # self.remote.create_from_file("cloud/operator/2/1/admin-user.yaml")
            self.remote.create_from_file("cloud/operator/2/1/bucket-user.yaml")
            self.remote.create_from_file("cloud/operator/2/1/rbac-admin-group.yaml")
            self.remote.create_from_file("cloud/operator/2/1/rbac-admin-role-binding.yaml")
        else:
            if not self.rest.supports_rbac(self.master_node):
                logger.info('RBAC not supported - skipping adding RBAC users')
                return

            if self.rest.is_community(self.master_node):
                roles = self.generate_ce_roles()
            else:
                roles = self.generate_ee_roles()

            for master in self.cluster_spec.masters:
                admin_user, admin_password = self.cluster_spec.rest_credentials
                self.rest.add_rbac_user(
                    host=master,
                    user=admin_user,
                    password=admin_password,
                    roles=['admin'],
                )

                buckets = self.test_config.buckets + self.test_config.eventing_buckets

                for bucket in buckets:
                    bucket_roles = [role.format(bucket=bucket) for role in roles]
                    bucket_roles.append("admin")
                    self.rest.add_rbac_user(
                        host=master,
                        user=bucket,  # Backward compatibility
                        password=self.test_config.bucket.password,
                        roles=bucket_roles,
                    )

    def add_extra_rbac_users(self, num_users):
        if not self.rest.supports_rbac(self.master_node):
            logger.info('RBAC not supported - skipping adding RBAC users')
            return

        if self.rest.is_community(self.master_node):
            roles = self.generate_ce_roles()
        else:
            roles = self.generate_ee_roles()

        for master in self.cluster_spec.masters:
            admin_user, admin_password = self.cluster_spec.rest_credentials
            self.rest.add_rbac_user(
                host=master,
                user=admin_user,
                password=admin_password,
                roles=['admin'],
            )

            for bucket in self.test_config.buckets:
                bucket_roles = [role.format(bucket=bucket) for role in roles]
                bucket_roles.append("admin")
                for i in range(1, num_users+1):
                    user = 'user{user_number}'.format(user_number=str(i))
                    self.rest.add_rbac_user(
                        host=master,
                        user=user,
                        password=self.test_config.bucket.password,
                        roles=bucket_roles,
                    )

    def throttle_cpu(self):
        if self.dynamic_infra:
            cluster = self.remote.get_cluster()
            if self.test_config.cluster.enable_cpu_cores:
                server_groups = cluster['spec']['servers']
                updated_server_groups = []
                default_cpu = 80
                for server_group in server_groups:
                    resources = server_group.get('resources', {})
                    limits = resources.get('limits', {})
                    limits['cpu'] = default_cpu
                    resources['limits'] = limits
                    server_group['resources'] = resources
                    updated_server_groups.append(server_group)
                cluster['spec']['servers'] = updated_server_groups

            if self.test_config.cluster.online_cores:
                server_groups = cluster['spec']['servers']
                updated_server_groups = []
                online_vcpus = self.test_config.cluster.online_cores * 2
                for server_group in server_groups:
                    resources = server_group.get('resources', {})
                    limits = resources.get('limits', {})
                    limits['cpu'] = online_vcpus
                    resources['limits'] = limits
                    server_group['resources'] = resources
                    updated_server_groups.append(server_group)
                cluster['spec']['servers'] = updated_server_groups
            self.remote.update_cluster_config(cluster, timeout=300, reboot=True)
        else:
            if self.remote.os == 'Cygwin':
                return

            if self.test_config.cluster.enable_cpu_cores:
                self.remote.enable_cpu()

            if self.test_config.cluster.online_cores:
                self.remote.disable_cpu(self.test_config.cluster.online_cores)

    def tune_memory_settings(self):
        kernel_memory = self.test_config.cluster.kernel_mem_limit
        if kernel_memory:
            if self.dynamic_infra:
                cluster = self.remote.get_cluster()
                server_groups = cluster['spec']['servers']
                tune_services = set()
                # CAO uses different service names than perfrunner
                for service in self.test_config.cluster.kernel_mem_limit_services:
                    if service == 'kv':
                        service = 'data'
                    elif service == 'n1ql':
                        service = 'query'
                    elif service == 'fts':
                        service = 'search'
                    elif service == 'cbas':
                        service = 'analytics'
                    tune_services.add(service)

                updated_server_groups = []
                default_mem = '128Gi'
                for server_group in server_groups:
                    services_in_group = set(server_group['services'])
                    resources = server_group.get('resources', {})
                    limits = resources.get('limits', {})
                    mem_limit = limits.get('memory', default_mem)
                    if services_in_group.intersection(tune_services) and kernel_memory != 0:
                        mem_limit = '{}Mi'.format(kernel_memory)
                    limits['memory'] = mem_limit
                    resources['limits'] = limits
                    server_group['resources'] = resources
                    updated_server_groups.append(server_group)

                cluster['spec']['servers'] = updated_server_groups
                self.remote.update_cluster_config(cluster, timeout=300, reboot=True)
            else:
                for service in self.test_config.cluster.kernel_mem_limit_services:
                    for server in self.cluster_spec.servers_by_role(service):
                        self.remote.tune_memory_settings(host_string=server,
                                                         size=kernel_memory)
                self.monitor.wait_for_servers()

    def reset_memory_settings(self):
        if self.dynamic_infra:
            return
        for service in self.test_config.cluster.kernel_mem_limit_services:
            for server in self.cluster_spec.servers_by_role(service):
                self.remote.reset_memory_settings(host_string=server)
        self.monitor.wait_for_servers()

    def flush_iptables(self):
        if self.dynamic_infra:
            return
        self.remote.flush_iptables()

    def clear_login_history(self):
        if self.dynamic_infra:
            return
        self.remote.clear_wtmp()

    def disable_wan(self):
        if self.dynamic_infra:
            return
        self.remote.disable_wan()

    def enable_ipv6(self):
        if self.dynamic_infra:
            return
        if self.test_config.cluster.ipv6:
            version, build_number = self.build.split('-')
            build = tuple(map(int, version.split('.'))) + (int(build_number),)

            if build < (6, 5, 0, 0):
                self.remote.update_ip_family_rest()
            else:
                self.remote.update_ip_family_cli()
            self.remote.enable_ipv6()

    def set_x509_certificates(self):
        if self.dynamic_infra:
            return
        logger.info('Setting x509 settings')
        if self.test_config.access_settings.ssl_mode == "auth":
            self.remote.setup_x509()
            for host in self.cluster_spec.servers:
                self.rest.upload_cluster_certificate(host)
            for host in self.cluster_spec.servers:
                self.rest.reload_cluster_certificate(host)
                self.rest.enable_certificate_auth(host)

    def set_cipher_suite(self):
        if self.dynamic_infra:
            return
        if self.test_config.access_settings.cipher_list:
            check_cipher_suit = self.rest.get_cipher_suite(self.master_node)
            logger.info('current cipher suit: {}'.format(check_cipher_suit))
            self.rest.set_cipher_suite(
                self.master_node, self.test_config.access_settings.cipher_list)
            check_cipher_suit = self.rest.get_cipher_suite(self.master_node)
            logger.info('new cipher suit: {}'.format(check_cipher_suit))

    def set_min_tls_version(self):
        if self.dynamic_infra:
            return
        if self.test_config.access_settings.min_tls_version:
            check_tls_version = self.rest.get_minimum_tls_version(self.master_node)
            logger.info('current tls version: {}'.format(check_tls_version))
            self.rest.set_minimum_tls_version(
                self.master_node,
                self.test_config.access_settings.min_tls_version
            )
            check_tls_version = self.rest.get_minimum_tls_version(self.master_node)
            logger.info('new tls version: {}'.format(check_tls_version))

    def get_debug_rpm_url(self):
        release, build_number = self.build.split('-')
        build = tuple(map(int, release.split('.'))) + (int(build_number),)
        if build > (7, 0, 0, 0):
            release = 'cheshire-cat'
        elif build > (6, 5, 0, 0) and build < (7, 0, 0, 0):
            release = 'mad-hatter'
        elif build < (6, 5, 0, 0):
            release = 'alice'
        centos_version = self.remote.detect_centos_release()

        rpm_url = 'http://latestbuilds.service.couchbase.com/builds/' \
                  'latestbuilds/couchbase-server/{}/{}/' \
                  'couchbase-server-enterprise-debuginfo-{}-centos{}.x86_64.rpm' \
                  ''.format(release, build_number, self.build, centos_version)
        return rpm_url

    def install_cb_debug_rpm(self):
        self.remote.install_cb_debug_rpm(url=self.get_debug_rpm_url())

    def enable_developer_preview(self):
        release, build_number = self.build.split('-')
        build = tuple(map(int, release.split('.'))) + (int(build_number),)
        if build > (7, 0, 0, 4698) or build < (0, 0, 0, 9999):
            self.remote.enable_developer_preview()
