from argparse import ArgumentParser

from fabric.api import cd, run
from logger import logger
from time import sleep

from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import all_servers, all_clients
from perfrunner.settings import ClusterSpec, TestConfig


class AerospikeInstaller:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, options):
        self.test_config = test_config
        self.cluster_spec = cluster_spec
        self.client_settings = self.test_config.client_settings.__dict__
        self.options = options
        self.remote = RemoteHelper(self.cluster_spec, options.verbose)
        self.cluster = ClusterManager(self.cluster_spec, self.test_config)

    @all_servers
    def install_aerospike(self):
        logger.info("Download Aerospike server package")
        run("wget -O aerospike.tgz -nc -nv https://www.aerospike.com/download/server/latest/artifact/el7")
        run("rm -rf aerospike-server-community")
        run("mkdir aerospike-server-community")
        run("tar -xvf aerospike.tgz -C aerospike-server-community --strip-components 1")

        logger.info("Install Aerospike server")
        with cd('./aerospike-server-community'):
            run("./asinstall")

    @all_servers
    def install_amc(self):
        logger.info("Download Aerospike Management Console package")
        url = 'https://github.com/aerospike-community/amc/releases/download/'
        package = '4.1.3/aerospike-amc-enterprise-4.1.3-1.x86_64.rpm'
        download_link = url + package
        run("wget -O aerospike-amc-enterprise.rpm -nc -nv {}".format(download_link))

        logger.info("Install Aerospike Management Console")
        run("rpm -ivh aerospike-amc-enterprise.rpm")

    @all_servers
    def start_aerospike(self):
        logger.info("Start Aerospike server")
        run("cp /root/aerospike/{} /etc/aerospike/aerospike.conf"
            .format(self.test_config.cluster.aerospike_conf))
        run("mkdir /var/log/aerospike")
        run("touch /var/log/aerospike/aerospike.log")
        run("systemctl start aerospike")
        sleep(10)

    @all_servers
    def start_amc(self):
        logger.info("Start Aerospike Management Console")
        run("cp /root/aerospike/amc.conf /etc/amc/amc.conf")
        run("/etc/init.d/amc start")


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')
    parser.add_argument('-t', '--test', dest='test_config_fname',
                        required=True,
                        help='path to test test configuration file')
    parser.add_argument('--verbose', dest='verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster settings')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname, override=args.override)
    test_config = TestConfig()
    test_config.parse(args.test_config_fname, override=args.override)

    aerospike = AerospikeInstaller(cluster_spec, test_config, args)

    aerospike.cluster.disable_wan()
    # aerospike.cluster.tune_memory_settings()
    # aerospike.cluster.enable_ipv6()
    aerospike.cluster.flush_iptables()

    aerospike.install_aerospike()
    aerospike.install_amc()
    aerospike.start_aerospike()
    aerospike.start_amc()


if __name__ == '__main__':
    main()
