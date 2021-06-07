import os
import shutil

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.worker import ycsb_data_load_task, ycsb_task
from perfrunner.tests import PerfTest


class YCSBTest(PerfTest):

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.init_ycsb(
                    repo=self.test_config.ycsb_settings.repo,
                    workload=self.test_config.ycsb_settings.aerospike_workload,
                    branch=self.test_config.ycsb_settings.branch,
                    worker_home=self.worker_manager.WORKER_HOME,
                    sdk_version=self.test_config.ycsb_settings.sdk_version)
        else:
            local.clone_git_repo(repo=self.test_config.ycsb_settings.repo,
                                 workload=self.test_config.ycsb_settings.aerospike_workload,
                                 branch=self.test_config.ycsb_settings.branch)

    def collect_export_files(self):
        if self.worker_manager.is_remote:
            shutil.rmtree("YCSB", ignore_errors=True)
            os.mkdir('YCSB')
            self.remote.get_export_files(self.worker_manager.WORKER_HOME)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=ycsb_data_load_task)

    def access(self, *args, **kwargs):
        PerfTest.access(self, task=ycsb_task)

    def run(self):
        self.download_ycsb()

        self.load()

        self.access()

        self.report_kpi()

        self.remote.asinfo()


class YCSBThroughputTest(YCSBTest):

    def _report_kpi(self):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput()
        )


class YCSBDurabilityThroughputTest(YCSBTest):

    def log_latency_percentiles(self, type: str, percentiles):
        for percentile in percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and type in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    logger.info("{}: {}".format(key, latency_dic[key]))

    def log_percentiles(self):
        logger.info("------------------")
        logger.info("Latency Percentiles")
        logger.info("-------READ-------")
        self.log_latency_percentiles("READ", [95, 96, 97, 98, 99])
        logger.info("------UPDATE------")
        self.log_latency_percentiles("UPDATE", [95, 96, 97, 98, 99])
        logger.info("------------------")

    def _report_kpi(self):
        self.collect_export_files()

        self.log_percentiles()

        for key, value in self.metrics.ycsb_get_max_latency().items():
            max_latency, _, _ = self.metrics.ycsb_slo_max_latency(key, value)
            logger.info("Max {} Latency: {}".format(key, max_latency))

        for key, value in self.metrics.ycsb_get_failed_ops().items():
            failures, _, _ = self.metrics.ycsb_failed_ops(key, value)
            logger.info("{} Failures: {}".format(key, failures))

        gcs, _, _ = self.metrics.ycsb_gcs()
        logger.info("Garbage Collections: {}".format(gcs))

        self.reporter.post(
            *self.metrics.ycsb_durability_throughput()
        )

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_slo_latency(key, latency_dic[key])
                    )


class YCSBLatencyTest(YCSBTest):

    def _report_kpi(self):
        self.collect_export_files()

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )

        if self.test_config.ycsb_settings.average_latency == 1:
            latency_dic = self.metrics.ycsb_get_latency(
                percentile=99)

            for key, value in latency_dic.items():
                if "Average" in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )
