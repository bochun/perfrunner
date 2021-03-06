import json
from argparse import ArgumentParser

import boto3
import yaml

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec


class Deployer:

    def __init__(self, infra_spec, options):
        self.options = options
        self.cluster_path = options.cluster
        self.infra_spec = infra_spec
        self.settings = self.infra_spec.infrastructure_settings
        self.clusters = self.infra_spec.infrastructure_clusters
        self.clients = self.infra_spec.infrastructure_clients
        self.utilities = self.infra_spec.infrastructure_utilities
        self.infra_config = self.infra_spec.infrastructure_config()
        self.generated_cloud_config_path = self.infra_spec.generated_cloud_config_path

    def deploy(self):
        raise NotImplementedError


class AWSDeployer(Deployer):

    def __init__(self, infra_spec, options):
        super().__init__(infra_spec, options)
        self.desired_infra = self.gen_desired_infrastructure_config()
        self.deployed_infra = {}
        self.ec2client = boto3.client('ec2')
        self.ec2 = boto3.resource('ec2')
        self.cloudformation_client = boto3.client('cloudformation')
        self.eksclient = boto3.client('eks')
        self.iamclient = boto3.client('iam')
        self.eks_cluster_role_path = "cloud/infrastructure/aws/eks/eks_cluster_role.yaml"
        self.eks_node_role_path = "cloud/infrastructure/aws/eks/eks_node_role.yaml"
        self.generated_kube_config_dir = "cloud/infrastructure/generated/kube_configs"
        self.ebs_csi_iam_policy_path = "cloud/infrastructure/aws/eks/ebs-csi-iam-policy.json"

    def gen_desired_infrastructure_config(self):
        desired_infra = {'k8s': {}, 'ec2': {}}
        k8s = self.infra_spec.infrastructure_section('k8s')
        if 'clusters' in list(k8s.keys()):
            desired_k8s_clusters = k8s['clusters'].split(',')
            for desired_k8s_cluster in desired_k8s_clusters:
                k8s_cluster_config = self.infra_spec.infrastructure_section(desired_k8s_cluster)
                for desired_node_group in k8s_cluster_config['node_groups'].split(','):
                    node_group_config = self.infra_spec.infrastructure_section(desired_node_group)
                    k8s_cluster_config[desired_node_group] = node_group_config
                desired_infra['k8s'][desired_k8s_cluster] = k8s_cluster_config
        ec2 = self.infra_spec.infrastructure_section('ec2')
        if 'clusters' in list(ec2.keys()):
            desired_ec2_clusters = ec2['clusters'].split(',')
            for desired_ec2_cluster in desired_ec2_clusters:
                ec2_cluster_config = self.infra_spec.infrastructure_section(desired_ec2_cluster)
                desired_infra['ec2'][desired_ec2_cluster] = ec2_cluster_config
        return desired_infra

    def write_infra_file(self):
        with open(self.generated_cloud_config_path, 'w+') as fp:
            json.dump(self.deployed_infra, fp, indent=4, sort_keys=True, default=str)

    def create_vpc(self):
        logger.info("Creating VPC...")
        response = self.ec2client.create_vpc(
            CidrBlock='10.0.0.0/16',
            AmazonProvidedIpv6CidrBlock=False,
            DryRun=False,
            InstanceTenancy='default',
            TagSpecifications=[
                {'ResourceType': 'vpc',
                 'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'}]}])
        self.deployed_infra['vpc'] = response['Vpc']
        self.write_infra_file()
        waiter = self.ec2client.get_waiter('vpc_available')
        waiter.wait(VpcIds=[self.deployed_infra['vpc']['VpcId']],
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.describe_vpcs(
            VpcIds=[self.deployed_infra['vpc']['VpcId']], DryRun=False)
        self.deployed_infra['vpc'] = response['Vpcs'][0]
        self.write_infra_file()

    def create_subnets(self):
        logger.info("Creating subnets...")
        subnets = 0
        self.deployed_infra['vpc']['subnets'] = {}
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            for az in ['us-west-2a', 'us-west-2b']:
                response = self.ec2client.create_subnet(
                    TagSpecifications=[
                        {'ResourceType': 'subnet',
                         'Tags': [
                             {'Key': 'Use',
                              'Value': 'CloudPerfTesting'},
                             {'Key': 'Role',
                              'Value': cluster_name},
                             {'Key': 'kubernetes.io/cluster/{}'.format(cluster_name),
                              'Value': 'shared'}]}],
                    AvailabilityZone=az,
                    CidrBlock='10.0.{}.0/24'.format(subnets+1),
                    VpcId=self.deployed_infra['vpc']['VpcId'],
                    DryRun=False)
                subnets += 1
                subnet_id = response['Subnet']['SubnetId']
                self.deployed_infra['vpc']['subnets'][subnet_id] = response['Subnet']
                self.write_infra_file()

        if len(self.desired_infra['ec2'].keys()) > 0:
            response = self.ec2client.create_subnet(
                TagSpecifications=[
                    {'ResourceType': 'subnet',
                     'Tags': [
                         {'Key': 'Use',
                          'Value': 'CloudPerfTesting'},
                         {'Key': 'Role',
                          'Value': 'ec2'}]}],
                AvailabilityZone=az,
                CidrBlock='10.0.{}.0/24'.format(subnets+1),
                VpcId=self.deployed_infra['vpc']['VpcId'],
                DryRun=False)
            subnets += 1
            subnet_id = response['Subnet']['SubnetId']
            self.deployed_infra['vpc']['subnets'][subnet_id] = response['Subnet']
            self.write_infra_file()

        waiter = self.ec2client.get_waiter('subnet_available')
        waiter.wait(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()),
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.describe_subnets(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()), DryRun=False)
        for subnet in response['Subnets']:
            self.deployed_infra['vpc']['subnets'][subnet['SubnetId']] = subnet
        self.write_infra_file()

    def map_public_ip(self):
        logger.info("Mapping public IPs...")
        for subnet in list(self.deployed_infra['vpc']['subnets'].keys()):
            self.ec2client.modify_subnet_attribute(
                MapPublicIpOnLaunch={'Value': True},
                SubnetId=subnet)
        waiter = self.ec2client.get_waiter('subnet_available')
        waiter.wait(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()),
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.describe_subnets(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()), DryRun=False)
        for subnet in response['Subnets']:
            self.deployed_infra['vpc']['subnets'][subnet['SubnetId']] = subnet
        self.write_infra_file()

    def create_internet_gateway(self):
        logger.info("Creating internet gateway...")
        spec = {
            'ResourceType': 'internet-gateway',
            'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'}]}
        response = self.ec2client.create_internet_gateway(
            TagSpecifications=[spec],
            DryRun=False
        )
        self.deployed_infra['vpc']['internet_gateway'] = response['InternetGateway']
        self.write_infra_file()

    def attach_internet_gateway(self):
        logger.info("Attaching internet gateway...")
        self.ec2client.attach_internet_gateway(
            DryRun=False,
            InternetGatewayId=self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId'],
            VpcId=self.deployed_infra['vpc']['VpcId'])
        response = self.ec2client.describe_internet_gateways(
            DryRun=False,
            InternetGatewayIds=[
                self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId']])
        self.deployed_infra['vpc']['internet_gateway'] = response['InternetGateways'][0]
        self.write_infra_file()

    def create_public_routes(self):
        logger.info("Creating public routes...")
        response = self.ec2client.describe_route_tables(
            Filters=[{'Name': 'vpc-id',
                      'Values': [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['vpc']['route_tables'] = response['RouteTables']
        rt_updated = False
        for rt in self.deployed_infra['vpc']['route_tables']:
            if rt['VpcId'] == self.deployed_infra['vpc']['VpcId']:
                response = self.ec2client.create_route(
                    DestinationCidrBlock='0.0.0.0/0',
                    GatewayId=self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId'],
                    RouteTableId=rt['RouteTableId'])
                rt_updated = bool(response['Return'])
        if not rt_updated:
            raise Exception("Failed to update route table")
        response = self.ec2client.describe_route_tables(
            Filters=[{'Name': 'vpc-id',
                      'Values': [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['vpc']['route_tables'] = response['RouteTables']
        self.write_infra_file()

    def create_eks_roles(self):
        logger.info("Creating cloudformation eks roles...")
        with open(self.eks_cluster_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName='CloudPerfTestingEKSClusterRole',
                TemplateBody=cft_template,
                Capabilities=['CAPABILITY_IAM'],
                DisableRollback=True,
                EnableTerminationProtection=False)
            self.deployed_infra['vpc']['eks_cluster_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        with open(self.eks_node_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName='CloudPerfTestingEKSNodeRole',
                TemplateBody=cft_template,
                Capabilities=['CAPABILITY_IAM'],
                DisableRollback=True,
                EnableTerminationProtection=False)
            self.deployed_infra['vpc']['eks_node_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        waiter = self.cloudformation_client.get_waiter('stack_create_complete')
        waiter.wait(
            StackName='CloudPerfTestingEKSClusterRole',
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        waiter.wait(
            StackName='CloudPerfTestingEKSNodeRole',
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.cloudformation_client.describe_stacks(
            StackName='CloudPerfTestingEKSClusterRole')
        self.deployed_infra['vpc']['eks_cluster_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.write_infra_file()
        response = self.cloudformation_client.describe_stacks(
            StackName='CloudPerfTestingEKSNodeRole')
        self.deployed_infra['vpc']['eks_node_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.write_infra_file()

    def create_eks_clusters(self):
        logger.info("Creating eks clusters...")
        self.deployed_infra['vpc']['eks_clusters'] = {}
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            cluster_version = self.infra_spec.kubernetes_version(cluster_name)
            eks_subnets = []
            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            response = self.eksclient.create_cluster(
                name=cluster_name,
                version=cluster_version,
                roleArn=self.deployed_infra['vpc']['eks_cluster_role_iam_arn'],
                resourcesVpcConfig={
                    'subnetIds': eks_subnets,
                    'endpointPublicAccess': True,
                    'endpointPrivateAccess': False},
                kubernetesNetworkConfig={'serviceIpv4Cidr': '172.{}.0.0/16'.format(20+i)},
                tags={'Use': 'CloudPerfTesting', 'Role': cluster_name})
            self.deployed_infra['vpc']['eks_clusters'][response['cluster']['name']] = \
                response['cluster']
            self.write_infra_file()
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            waiter = self.eksclient.get_waiter('cluster_active')
            waiter.wait(name=cluster_name,
                        WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
            self.deployed_infra['vpc']['eks_clusters'][response['cluster']['name']] = \
                response['cluster']
            self.write_infra_file()
            self.create_kubeconfig(cluster_name)

    def create_kubeconfig(self, cluster_name):
        cluster = self.eksclient.describe_cluster(name=cluster_name)
        cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
        cluster_ep = cluster["cluster"]["endpoint"]
        cluster_arn = cluster["cluster"]["arn"]
        cluster_config = {
            "apiVersion": "v1",
            "clusters": [
                {"cluster": {"server": str(cluster_ep),
                             "certificate-authority-data": str(cluster_cert)},
                 "name": str(cluster_arn)}],
            "users":
                [{"name": str(cluster_arn),
                  "user":
                      {"exec":
                          {"apiVersion": "client.authentication.k8s.io/v1alpha1",
                           "command": "aws",
                           "args":
                               ["--region",
                                "us-west-2",
                                "eks",
                                "get-token",
                                "--cluster-name",
                                cluster_name]}}}],
            "contexts":
                [{"context":
                    {"cluster": str(cluster_arn),
                     "user": str(cluster_arn)},
                  "name": str(cluster_arn)}],
            "current-context": str(cluster_arn)}
        config_path = '{}/{}'.format(self.generated_kube_config_dir, cluster_name)
        with open(config_path, 'w+') as fp:
            yaml.dump(cluster_config, fp, default_flow_style=False)
        self.deployed_infra['vpc']['eks_clusters'][cluster_name]['kube_config'] = cluster_config
        self.deployed_infra['vpc']['eks_clusters'][cluster_name]['kube_config_path'] = config_path
        self.write_infra_file()

    def create_eks_node_groups(self):
        logger.info("Creating eks node groups...")
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            cluster_infra = self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name]
            cluster_infra['node_groups'] = {}
            eks_subnets = []
            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == k8s_cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                resource_path = 'k8s.{}.{}'.format(k8s_cluster_name, node_group)
                labels = {'NodeRoles': None}
                for k, v in self.clusters.items():
                    if 'couchbase' in k:
                        for host in v.split():
                            host_resource, services = host.split(":")
                            if resource_path in host_resource:
                                labels['NodeRoles'] = k
                                for service in services.split(","):
                                    labels['{}_enabled'.format(service)] = 'true'
                for k, v in self.clients.items():
                    if 'workers' in k and resource_path in v:
                        labels['NodeRoles'] = k
                        break
                    if 'backups' in k and resource_path in v:
                        labels['NodeRoles'] = k
                        break
                for k, v in self.utilities.items():
                    if ('brokers' in k or 'operators' in k) and resource_path in v:
                        labels['NodeRoles'] = 'utilities'
                node_group_spec = k8s_cluster_spec[node_group]
                response = self.eksclient.create_nodegroup(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group,
                    scalingConfig={
                        'minSize': int(node_group_spec['instance_capacity']),
                        'maxSize': int(node_group_spec['instance_capacity']),
                        'desiredSize': int(node_group_spec['instance_capacity'])
                    },
                    diskSize=int(node_group_spec['volume_size']),
                    subnets=eks_subnets,
                    instanceTypes=[node_group_spec['instance_type']],
                    amiType='AL2_x86_64',
                    remoteAccess={'ec2SshKey': self.infra_spec.aws_key_name},
                    nodeRole=self.deployed_infra['vpc']['eks_node_role_iam_arn'],
                    labels=labels,
                    tags={'Use': 'CloudPerfTesting',
                          'Role': k8s_cluster_name,
                          'SubRole': node_group})
                cluster_infra['node_groups'][node_group] = response['nodegroup']
                self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name] = cluster_infra
                self.write_infra_file()
        waiter = self.eksclient.get_waiter('nodegroup_active')
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                waiter.wait(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group,
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            cluster_infra = self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name]
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                response = self.eksclient.describe_nodegroup(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group)
                cluster_infra['node_groups'][node_group] = response['nodegroup']
                self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name] = cluster_infra
                self.write_infra_file()

    def create_ec2s(self):
        logger.info("Creating ec2s...")
        self.deployed_infra['vpc']['ec2'] = {}
        if len(list(self.desired_infra['ec2'].keys())) > 0:
            ec2_subnet = None
            for subnet_name, subnet_config in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_config['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == 'ec2':
                        ec2_subnet = subnet_name
                        break
                if ec2_subnet is not None:
                    break
            if ec2_subnet is None:
                raise Exception("need at least one subnet with tag ec2 to deploy instances")
            for ec2_group_name, ec2_config in self.desired_infra['ec2'].items():
                if "client" in ec2_group_name:
                    ami = 'ami-dbf9baa3'  # perf client ami
                elif "server" in ec2_group_name:
                    ami = 'ami-83b400fb'  # perf server ami
                elif "broker" in ec2_group_name:
                    ami = 'ami-dbf9baa3'  # perf client ami
                else:
                    raise Exception("ec2 group must include one of: client, server, broker")
                response = self.ec2.create_instances(
                    BlockDeviceMappings=[
                        {'DeviceName': '/dev/sda1',
                         'Ebs':
                             {'DeleteOnTermination': True,
                              'VolumeSize': int(ec2_config['volume_size']),
                              'VolumeType': 'gp2',
                              'Encrypted': False}}],
                    ImageId=ami,
                    InstanceType=ec2_config['instance_type'],
                    KeyName=self.infra_spec.aws_key_name,
                    MaxCount=int(ec2_config['instance_capacity']),
                    MinCount=int(ec2_config['instance_capacity']),
                    Monitoring={'Enabled': False},
                    SubnetId=ec2_subnet,
                    DisableApiTermination=False,
                    DryRun=False,
                    EbsOptimized=False,
                    InstanceInitiatedShutdownBehavior='terminate',
                    TagSpecifications=[
                        {'ResourceType': 'instance',
                         'Tags':
                             [{'Key': 'Use', 'Value': 'CloudPerfTesting'},
                              {'Key': 'Role', 'Value': ec2_group_name}]}])
                ec2_group = self.deployed_infra['vpc']['ec2'].get(ec2_group_name, [])
                ec2_group.append(response[0].id)
                self.deployed_infra['vpc']['ec2'][ec2_group_name] = ec2_group
                self.write_infra_file()

            for ec2_group_name, ec2_list in self.deployed_infra['vpc']['ec2'].items():
                    waiter = self.ec2client.get_waiter('instance_status_ok')
                    waiter.wait(
                        InstanceIds=ec2_list,
                        DryRun=False,
                        WaiterConfig={'Delay': 10, 'MaxAttempts': 600})

    def open_security_groups(self):
        logger.info("Opening security groups...")
        response = self.ec2client.describe_security_groups(
            Filters=[
                {'Name': 'vpc-id',
                 'Values':
                     [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['security_groups'] = response['SecurityGroups']
        self.write_infra_file()
        for sg in self.deployed_infra['security_groups']:
            self.ec2client.authorize_security_group_ingress(
                GroupId=sg['GroupId'],
                IpPermissions=[
                    {'FromPort': -1,
                     'IpProtocol': '-1',
                     'IpRanges':
                         [{'CidrIp': '0.0.0.0/0'}],
                     'ToPort': -1}])
        response = self.ec2client.describe_security_groups(
            Filters=[
                {'Name': 'vpc-id',
                 'Values':
                     [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['security_groups'] = response['SecurityGroups']
        self.write_infra_file()

    def setup_eks_csi_driver_iam_policy(self):
        logger.info("Attaching EBS CSI Driver policy ARN...")
        with open(self.ebs_csi_iam_policy_path) as f:
            self.iam_policy = json.load(f)
        self.iam_policy = json.dumps(self.iam_policy)
        response = self.iamclient.create_policy(
            PolicyName='CloudPerfTesting-Amazon_EBS_CSI_Driver',
            PolicyDocument=self.iam_policy,
            Description='Cloud Perf Testing IAM Policy to enable EKS EBS Persistent Volumes'
        )
        self.deployed_infra['vpc']['ebs_csi_policy_arn'] = response['Policy']['Arn']
        self.write_infra_file()
        self.iamclient.attach_role_policy(
            RoleName=self.deployed_infra['vpc']['eks_node_role_iam_arn'].split("/")[1],
            PolicyArn=self.deployed_infra['vpc']['ebs_csi_policy_arn']
        )

    def update_infrastructure_spec(self):
        remote = RemoteHelper(self.infra_spec)

        with open(self.generated_cloud_config_path) as f:
            self.deployed_infra = json.load(f)

        k8_nodes = {
            node_dict['metadata']['name']:
                {
                    "labels": node_dict['metadata']['labels'],
                    "addresses": node_dict['status']['addresses']
                }
            for node_dict in remote.get_nodes()}

        address_replace_list = []
        clusters = self.infra_spec.infrastructure_clusters
        for cluster, hosts in clusters.items():
            for host in hosts.split():
                address, services = host.split(":")
                node_group = address.split(".")[2]
                matching_node = None
                for node_name, node_spec in k8_nodes.items():
                    if node_spec['labels']['NodeRoles'] != cluster:
                        continue
                    if node_spec['labels']['eks.amazonaws.com/nodegroup'] != node_group:
                        continue

                    has_all_services = True
                    for service in services.split(","):
                        service_enabled = node_spec['labels'].get("{}_enabled"
                                                                  .format(service), 'false')
                        if service_enabled != 'true':
                            has_all_services = False

                    if has_all_services:
                        replace_addr = None
                        for node_addr_dict in node_spec['addresses']:
                            if node_addr_dict['type'] == "ExternalIP":
                                replace_addr = node_addr_dict['address']
                        if not replace_addr:
                            raise Exception("no replace address found")
                        address_replace_list.append((address, replace_addr))
                        del k8_nodes[node_name]
                        matching_node = node_name
                        break
                if not matching_node:
                    raise Exception("no matching node found")

            print("cluster: {}, hosts: {}".format(cluster, str(address_replace_list)))

            # Safely read the input filename using 'with'
            with open(self.cluster_path) as f:
                s = f.read()
            # Safely write the changed content, if found in the file
            with open(self.cluster_path, 'w') as f:
                for replace_pair in address_replace_list:
                    s = s.replace(replace_pair[0], replace_pair[1])
                f.write(s)

    def deploy(self):
        logger.info("Deploying infrastructure...")
        self.create_vpc()
        self.create_subnets()
        self.map_public_ip()
        self.create_internet_gateway()
        self.attach_internet_gateway()
        self.create_public_routes()
        self.create_eks_roles()
        self.create_eks_clusters()
        self.create_eks_node_groups()
        self.create_ec2s()
        self.open_security_groups()
        self.update_infrastructure_spec()
        if self.deployed_infra['vpc'].get('eks_clusters', None) is not None:
            for k, v in self.deployed_infra['vpc']['eks_clusters'].items():
                logger.info("eks cluster {} kube_config available at: {}"
                            .format(k, v['kube_config_path']))
        logger.info("Infrastructure deployment complete")


class AzureDeployer(Deployer):

    def deploy(self):
        pass


class GCPDeployer(Deployer):

    def deploy(self):
        pass


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')

    return parser.parse_args()


def main():
    args = get_args()

    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)

    if infra_spec.dynamic_infrastructure:
        infra_provider = infra_spec.infrastructure_settings['provider']

        if infra_provider == 'aws':
            deployer = AWSDeployer(infra_spec, args)
        elif infra_provider == 'azure':
            deployer = AzureDeployer(infra_spec, args)
        elif infra_provider == 'gcp':
            deployer = GCPDeployer(infra_spec, args)
        else:
            raise Exception("{} is not a valid infrastructure provider".format(infra_provider))

        try:
            deployer.deploy()
        except Exception as ex:
            with open(infra_spec.generated_cloud_config_path) as f:
                logger.info("infrastructure dump:\n{}".format(pretty_dict(json.load(f))))
            raise ex


if __name__ == '__main__':
    main()
