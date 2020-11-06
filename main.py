# coding=utf-8

import json
import logging
import tempfile
import os
import getopt
import sys


from aliyunsdkcs.request.v20151215 import DescribeClusterDetailRequest
from aliyunsdkcs.request.v20151215 import DescribeClusterUserKubeconfigRequest
from aliyunsdkcs.request.v20151215 import ListTagResourcesRequest, ModifyClusterTagsRequest
from aliyunsdkcore import client as AliyunClient
from aliyunsdkecs.request.v20140526 import DescribeDisksRequest, DescribeSnapshotsRequest, DescribeNetworkInterfacesRequest, DescribeInstancesRequest, TagResourcesRequest
from aliyunsdkslb.request.v20140515 import DescribeLoadBalancersRequest
from aliyunsdkslb.request.v20140515 import TagResourcesRequest as TagSLBResourcesRequest
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException


from kubernetes import client, config

batch_size = 50
aliyunClient = None

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def find_by_path(obj, *args):
    for arg in args:
        obj = obj.get(arg)
        if obj == None:
            return None
    return obj

def tag_exists(resource, key, value):
    tags = find_by_path(resource, 'Tags', 'Tag')
    for tag in tags:
        if tag.get('TagKey') == key and tag.get('TagValue') == value:
            return True
        
def do_action(req):
    try:
        req.set_accept_format('json')
        resp = aliyunClient.do_action_with_exception(req)
    except Exception as e:
        # TODO handle throttling
        logging.error(e)
        return
    return json.loads(str(resp, encoding='utf-8'))

class NodeInfo(object):
    def __init__(self, node_info):
        metadata = node_info.metadata
        spec = node_info.spec
        self.name = metadata.name
        provider_id = spec.provider_id
        provider_info = provider_id.split('.')
        self.region = provider_info[0]
        self.instance_id = provider_info[1]
        self.zone = metadata.labels['failure-domain.beta.kubernetes.io/zone']
        self.disks = self.get_disks()
        self.snapshots = self.get_snapshots()
        self.network_interfaces = self.get_network_interfaces()
        self.info = None
    
    def __repr__(self):
        return "region: {0}, zone: {1}, name: {2}, instance_id: {3}, \ndisks: {4}, \nsnapshots: {5}\nnetwork_interfaces: {6}\ninfo: {7}".format(self.region, self.zone, self.name, self.instance_id, self.disks, self.snapshots, self.network_interfaces, self.info)

    def get_disks(self):
        req = DescribeDisksRequest.DescribeDisksRequest()
        req.set_InstanceId(self.instance_id)
        req.set_PageSize(20)
        req.set_PageNumber(1)
        resp = do_action(req)
        return find_by_path(resp, 'Disks', 'Disk')

    def get_snapshots(self):
        req = DescribeSnapshotsRequest.DescribeSnapshotsRequest()
        req.set_InstanceId(self.instance_id)
        resp = do_action(req)
        return find_by_path(resp, 'Snapshots', 'Snapshot')

    def get_network_interfaces(self):
        req = DescribeNetworkInterfacesRequest.DescribeNetworkInterfacesRequest()
        req.set_InstanceId(self.instance_id)
        resp = do_action(req)
        return find_by_path(resp, 'NetworkInterfaceSets', 'NetworkInterfaceSet')

    def tag_resource(self, type, id, key, value):
        req = TagResourcesRequest.TagResourcesRequest()
        req.set_ResourceType(type)
        req.set_ResourceIds([id])
        req.set_Tags([{"Key":key, "Value":value}])
        resp = do_action(req)
        return resp != None

    def tag_resources(self, key, value, save):
        if tag_exists(self.info, key, value):
            logging.info("ECS instance %s is tagged" % self.instance_id)
        else:
            logging.info("ECS instance %s is not tagged" % self.instance_id)
            if save:
                if self.tag_resource("instance", self.instance_id, key, value):
                    logging.info("ECS instance %s is tagged successfully" % self.instance_id)
                else:
                    logging.error("Failed to tag ECS instance %s" % self.instance_id)
        
        for disk in self.disks:
            disk_id = disk.get('DiskId')
            if tag_exists(disk, key, value):
                logging.info("Disk %s is tagged" % disk_id)
            else:
                logging.info("Disk %s is not tagged" % disk_id)
                if save:
                    if self.tag_resource("disk", disk_id, key, value):
                        logging.info("Disk %s is tagged successfully" % disk_id)
                    else:
                        logging.error("Failed to tag Disk %s" % disk_id)

        for snapshot in self.snapshots:
            snapshot_id = snapshot.get('SnapshotId')
            if tag_exists(snapshot, key, value):
                logging.info("Snapshot %s is tagged" % snapshot_id)
            else:
                logging.info("Snapshot %s is not tagged" % snapshot_id)
                if save:
                    if self.tag_resource("snapshot", snapshot_id, key, value):
                        logging.info("Snapshot %s is tagged successfully" % snapshot_id)
                    else:
                        logging.error("Failed to tag snapshot %s" % snapshot_id)


        for network_interface in self.network_interfaces:
            network_interface_id = network_interface.get('NetworkInterfaceId')
            if tag_exists(network_interface, key, value):
                logging.info("Network interface %s is tagged" % network_interface_id)
            else:
                logging.info("Network interface %s is not tagged" % network_interface_id)
                if save:
                    if self.tag_resource("eni", network_interface_id, key, value):
                        logging.info("Network interface %s is tagged successfully" % network_interface_id)
                    else:
                        logging.error("Failed to tag network interface %s" % network_interface_id)


def _add_ecs_instances_to_map(instance_map, node_id_list):
    req = DescribeInstancesRequest.DescribeInstancesRequest()
    req.set_InstanceIds(json.dumps(node_id_list))
    resp = do_action(req)
    logging.debug(resp)
    if resp is not None:
        instance_list = resp.get('Instances').get('Instance')
        for item in instance_list:
            instance_id = item.get('InstanceId')
            instance_map[instance_id] = item
    return instance_map

def destribe_ecs_instances(nodes):
    count = 0
    node_id_list = []
    instance_map = {}
    for node in nodes:
        logging.debug(node)
        node_id_list.append(node.instance_id)
        count += 1
        if count == batch_size:
            instance_map =  _add_ecs_instances_to_map(instance_map, node_id_list)
            count = 0
            node_id_list = []
    if count > 0:
        instance_map =  _add_ecs_instances_to_map(instance_map, node_id_list)
    for node in nodes:
        node.info = instance_map[node.instance_id]

def load_kube_config(cluster_id):
    req = DescribeClusterUserKubeconfigRequest.DescribeClusterUserKubeconfigRequest()
    req.set_ClusterId(cluster_id)
    status, header, resp = aliyunClient.get_response(req)
    if status == 200:
        kubeconfig = json.loads(resp)
        logging.debug(kubeconfig["config"])
        fd, path = tempfile.mkstemp()
        os.write(fd, kubeconfig["config"].encode())
        os.close(fd)
        config.load_kube_config(path)
        os.remove(path)
        return True
    else:
        logging.error("Failed to retrieve the cluster %s certs: %d" % (cluster_id, status))
        return False

def list_nodes():
    nodes = []
    v1 = client.CoreV1Api()
    ret = v1.list_node()
    for item in ret.items:
        node = NodeInfo(item)
        nodes.append(node)

    destribe_ecs_instances(nodes)
    return nodes

def tag_slb_instances(cluster_id, tag_key, tag_value, save):
    tags = [{"TagKey":"ack.aliyun.com","TagValue":cluster_id}]
    req = DescribeLoadBalancersRequest.DescribeLoadBalancersRequest()
    req.set_Tags = json.dumps(tags)
    resp = do_action(req)
    logging.debug(resp)
    list = resp.get('LoadBalancers').get('LoadBalancer')

    for item in list:
        instance_id = item.get("LoadBalancerId")
        if tag_exists(item, tag_key, tag_value):
            logging.info("SLB instance %s is tagged" % instance_id)
        else:
            logging.info("SLB instance %s is not tagged" % instance_id)
            if save:
                req = TagSLBResourcesRequest.TagResourcesRequest()
                req.set_ResourceType('instance')
                req.set_ResourceIds([instance_id])
                req.set_Tags([{"Key":tag_key, "Value":tag_value}])
                resp = do_action(req)
                if resp != None:
                    logging.info("SLB instance %s is tagged successfully" % instance_id)
                else:
                    logging.error("Failed to tag SLB instance %s" % instance_id)

def tag_cluster(cluster_id, tag_key, tag_value, save):
    req = DescribeClusterDetailRequest.DescribeClusterDetailRequest()
    req.set_ClusterId(cluster_id)
    status, header, resp = aliyunClient.get_response(req)
    tags = []
    if status == 200:
        cluster = json.loads(resp)
        tags = cluster.get('tags')
    else:
        logging.error("Failed to get the details of cluster %s: %d" % (cluster_id, status))
        return False
    found = False
    merged = False
    for tag in tags:
        if tag.get('key') == tag_key:
            if tag.get('value') == tag_value:
                found = True
                break
            else:
                tag['value'] = tag_value
                merged = True
    if found:
        logging.info("Cluster instance %s is tagged" % cluster_id)
    else:
        logging.info("Cluster instance %s is not tagged" % cluster_id)
        mctReq = ModifyClusterTagsRequest.ModifyClusterTagsRequest()
        mctReq.set_ClusterId(cluster_id)
        if not merged:
            tags.append({'key': tag_key, 'value': tag_value})
        json_object = json.dumps(tags)
        mctReq.set_content_type("application/json;chrset=utf-8")
        mctReq.set_content(json_object)
        try:
            aliyunClient.do_action_with_exception(mctReq)
            logging.info("Cluster instance %s is tagged successfully" % cluster_id)
        except Exception as e:
            logging.error("Failed to tag cluster instance %s" % cluster_id)
            logging.error(e)

def list_lb_services():
    services = []
    v1 = client.CoreV1Api()
    ret = v1.list_service_for_all_namespaces()
    for item in ret.items:
        if item.spec.type == 'LoadBalancer':
            services.append(item)

    return services

def help():
    print('python3 tag_ack_cluster_resources.py -h|--help')
    print('python3 tag_ack_cluster_resources.py [-r|--region=cn_hangzhou] [-c|--cluster_id=xxx] [-k|--key=ack.aliyun.com] [-v|--value=xxx] [-s|--save]')
    sys.exit(1)

if __name__ == '__main__':
    access_key_id = os.environ.get("ACCESS_KEY_ID")
    access_key_secret = os.environ.get("ACCESS_KEY_SECRET")
    region = os.environ.get("REGION", "cn-hangzhou")
    cluster_id = os.environ.get("ACK_CLUSTER_ID")
    tag_key = ''
    tag_value = ''
    save = False

    try:
        (options, args) = getopt.getopt(sys.argv[1:], '-r:-c:-k:-v:-s-h', ['region=', 'cluster_id=', 'key=', 'value=', 'save', 'help'])
    except getopt.GetoptError:
        help()
    
    for option in options:
        if option[0] == '-r' or option[0] == '--region':
            region = option[1]
        elif option[0] == '-c' or option[0] == '--cluster_id':
            cluster_id = option[1]
        elif option[0] == '-k' or option[0] == '--key':
            tag_key = option[1]
        elif option[0] == '-v' or option[0] == '--value':
            tag_value = option[1]
        elif option[0] == '-s' or option[0] == '--save':
            save = True
        elif option[0] == '-h' or option[0] == '--help':
            help()

    if not access_key_id:
        logging.error("Please set the environment variable of ACCESS_KEY_ID")
        sys.exit(1)

    if not access_key_secret:
        logging.error("Please set the environment variable of ACCESS_KEY_SECRET")
        sys.exit(1)
    
    if not region:
        logging.error("Please set the environment variable of REGION or add --cluster param in CLI")
        sys.exit(1)
    
    if not cluster_id:
        logging.error("Please set the environment variable of ACK_CLUSTER_ID or add --cluster_id param in CLI")
        sys.exit(1)

    if not tag_key:
        logging.error("Please add --key param in CLI")
        sys.exit(1)
    
    if not tag_value:
        logging.error("Please add --value param in CLI")
        sys.exit(1)

    logging.info("cluster_id = %s" % cluster_id)
    logging.info("region = %s" % region)
    logging.info("save = %r" % save)

    aliyunClient = AliyunClient.AcsClient(access_key_id, access_key_secret, region)

    tag_cluster(cluster_id, tag_key, tag_value, save)
    if load_kube_config(cluster_id):        
        nodes = list_nodes()
        for node in nodes:
            node.tag_resources(tag_key, tag_value, save)
        tag_slb_instances(cluster_id, tag_key, tag_value, save)
        
