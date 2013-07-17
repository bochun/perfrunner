from hashlib import md5

from logger import logger

from perfrunner.helpers.cbmonitor import CbAgent
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import WorkerManager
from perfrunner.settings import TargetSettings


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            for bucket in self.test_config.get_buckets():
                prefix = target_hash(master, bucket)
                yield TargetSettings(master, bucket, username, password, prefix)


class PerfTest(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)

        self.cbagent = CbAgent(cluster_spec)
        self.monitor = Monitor(cluster_spec, test_config)
        self.rest = RestHelper(cluster_spec)
        self.reporter = Reporter(self)
        self.remote = RemoteHelper(cluster_spec)
        self.worker_manager = WorkerManager(cluster_spec)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.worker_manager.terminate()
        self.debug()

    def compact_bucket(self):
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node,
                                                target.bucket)
            self.monitor.monitor_task(target, 'bucket_compaction')

    def _run_workload(self, settings, target_iterator=None):
        if target_iterator is None:
            target_iterator = self.target_iterator
        self.worker_manager.run_workload(settings, target_iterator)

    def _wait_for_persistence(self):
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)
            self.monitor.monitor_tap_replication(target)

    def run_load_phase(self):
        load_settings = self.test_config.get_load_settings()
        logger.info('Running load phase: {0}'.format(load_settings))
        self._run_workload(load_settings)
        self._wait_for_persistence()

    def run_access_phase(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running access phase: {0}'.format(access_settings))
        self._run_workload(access_settings)

    def debug(self):
        self.remote.collect_info()
        self.reporter.save_web_logs()
