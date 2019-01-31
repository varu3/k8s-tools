# coding: utf-8
# !/usr/bin/env python

import os
import re
import sys
from itertools import chain
from pprint import pprint

sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))

from kubernetes.stream import stream
from kubernetes.client.rest import ApiException
from kubernetes.client.apis import core_v1_api
from kubernetes.client import Configuration
from kubernetes import config

HOST_PATH = [
    '/calico/v1/host/',
    '/calico/bgp/v1/host/',
    '/calico/ipam/v2/host/'
]


class EtcdCleaner:
    def __init__(self):
        config.load_kube_config()
        con = Configuration()
        con.assert_hostname = False
        Configuration.set_default(con)

        self.v1 = core_v1_api.CoreV1Api()
        self.CalicoNodePods = []
        self.EtcdServerPods = []
        self.k8sNodes = []
        self.EtcdNodes = {}

        self.rmNodes = []

    def Exec(self, dryrun=True):
        self.listK8sNodes()
        self.listEtcdNodes()
        self.diffNodes()

        print '\nKubernetes Nodes(nodes joining k8s cluster) %s nodes: ' % len(self.k8sNodes)
        pprint(self.k8sNodes)

        print '\nEtcd Nodes(nodes registered in etcd) %s nodes: ' % len(self.EtcdNodes)
        for path in HOST_PATH:
            print '%s: %s' % (path, len(self.EtcdNodes[path]))
        pprint(self.EtcdNodes)

        print '\nRemove Nodes(not used nodes) %s nodes: ' % len(self.rmNodes)
        pprint(self.rmNodes)

        if dryrun is True:
            exit(0)
        else:
            etcdPod = self.EtcdServerPods[0]
            for rmNode in self.rmNodes:
                exec_cmd = ['etcdctl', 'rm', '--recursive', rmNode]

                print '\n%s:' % etcdPod + ' '.join(exec_cmd)
                res = self.__k8sExecPod(etcdPod, None, 'kube-system', exec_cmd)
                pprint(res)

    def diffNodes(self):
        d = {}
        for path in HOST_PATH:
            for k8s_node in self.k8sNodes:
                # k8sのnodeがETCDにない場合
                if k8s_node in set(self.EtcdNodes[path]) is False:
                    print '[warn] %s is not in %s' % (k8s_node, path)
                    exit(1)

                # たまにhostnameがip-XX-XX-XX-XXそのままetcdに登録されている
                # ip-XX-XX-XX-XXのhostnameのまま登録されていた場合はprivateDNSを通知する
                p = self.__hostnameToPrivateDNS(k8s_node)
                if p is None:
                    continue

                if p in set(self.EtcdNodes[path]):
                    print '[warn] But, %s is in %s' % (p, path)
                    continue

            d[path] = list(set(self.EtcdNodes[path]) - set(self.k8sNodes))

        # etcdのパスに変換する
        for k, nodes in d.items():
            for v in nodes:
                self.rmNodes.append(k + v)

        return self.rmNodes

    def listK8sNodes(self):
        self.__setCalicoNodePods()
        resp = []
        exec_cmd = ['sh', '-c', 'hostname']
        for c in self.CalicoNodePods:
            resp.append(
                self.__k8sExecPod(c, 'calico-node', 'kube-system', exec_cmd))
        self.k8sNodes = list(chain.from_iterable(resp))
        return self.k8sNodes

    def listEtcdNodes(self):
        self.__setEtcdServerPods()
        etcdPod = self.EtcdServerPods[0]
        for path in HOST_PATH:
            exec_command = ['etcdctl', 'ls', path]
            resp = self.__k8sExecPod(etcdPod, None, 'kube-system',
                                     exec_command)
            self.EtcdNodes[path] = [x.split(path)[1] for x in resp]
        return self.EtcdNodes

    def __setEtcdServerPods(self):
        self.EtcdServerPods = self.__k8sGetPods('^etcd-server-ip.*')
        return self.EtcdServerPods

    def __setCalicoNodePods(self):
        self.CalicoNodePods = self.__k8sGetPods('^calico-node-[a-zA-Z0-9]{5}$')
        return self.CalicoNodePods

    def __k8sGetPods(self, pattern):
        pods = []
        try:
            for p in self.v1.list_pod_for_all_namespaces().items:
                if re.match(pattern, p.metadata.name):
                    pods.append(p.metadata.name)
        except ApiException as e:
            if e.status != 404:
                print 'Unknown error: %s' % e
                exit(1)

        if len(pods) == 0:
            print '/%s/ No pattern matched pods.' % pattern
            exit(1)
        return pods

    def __k8sExecPod(self, pod, container, namespace, exec_cmd):
        try:
            resp = self.v1.read_namespaced_pod(name=pod, namespace=namespace)
        except ApiException as e:
            if e.status != 404:
                print('Unknown error: %s' % e)
                exit(1)

        if container is not None:
            resp = stream(
                self.v1.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=exec_cmd,
                container=container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False)
        else:
            resp = stream(
                self.v1.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=exec_cmd,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False)

        return filter(lambda str: str != '', resp.split('\n'))

    def __hostnameToPrivateDNS(self, hostname):
        pattern = '\d+x\d+x\d+x\d+'
        ip = re.search(pattern, hostname)

        # patternがマッチしなかった場合
        if ip is None:
            print '[error] %s is not match %s' % (hostname, pattern)
            return None
        else:
            ip = ip.group(0).replace('x', '-')
            private_dns = 'ip-' + ip + '.ap-northeast-1.compute.internal'

        return private_dns


def main():
    dryrun = True

    args = sys.argv
    if len(args) > 1 and args[1] == 'apply':
        dryrun = False

    cleaner = EtcdCleaner()
    cleaner.Exec(dryrun=dryrun)


if __name__ == '__main__':
    main()
