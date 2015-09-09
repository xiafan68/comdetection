#coding:utf8
from redisinfo import CLUSTER_JOB_DB
import cPickle
from time import sleep
from ConfigParser import ConfigParser
import os
from dao.datalayer import DataLayer

class ClusterTask(object):
    def __init__(self, uid = -1, cnum=10, force=False):
        self.uid = uid
        self.cnum= cnum
        self.force= force
    
    def __str__(self):
        return ",".join(["%s=%s"%(f,v) for f,v in self.__dict__.items()])
"""
当前版本的queue实现是基于redis的，总是从redis读取任务
"""
class ClusterTaskQueue(object):
    def __init__(self, rCluster):
        self.redis = rCluster.getRedis("1", CLUSTER_JOB_DB)
    
    def addTask(self, task):
        self.redis.rpush('cluster', cPickle.dumps(task))
        
    def nextTask(self):
        tasktypes=['cluster']
        while True:
            for tasktype in tasktypes:
                ret = self.redis.lpop(tasktype)
                if ret:
                    ret =cPickle.loads(ret)
                    return (tasktype, ret)
            sleep(1)


def loadCompTopUsers():
    config = ConfigParser()
    cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
    config.read(cpath)
    datalayer = DataLayer(config)
    queue = ClusterTaskQueue(datalayer.getJobRedis())
    task = ClusterTask(1646586724, force=True)
    #1646586724 pingan
    #1897953162 ali
    #queue.addTask(task)
    import sys
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    from userquery import *
    from userquery.ttypes import *
    fd = open("/home/xiafan/KuaiPan/dataset/user/mhxkeyword.txt")
    for line in fd.readlines():
        fields = line.split("\t")
        transport = TSocket.TSocket('localhost', 10010)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        client = TweetService.Client(protocol)
        # Connect!
        transport.open()
        query=UserQuery(fields[0], 2)
        uids=client.search(query)
        for uid in uids:
            task = ClusterTask(uid=uid, force=True)
            queue.addTask(task)

def loadAllCompUsers():
    config = ConfigParser()
    cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
    config.read(cpath)
    datalayer = DataLayer(config)
    queue = ClusterTaskQueue(datalayer.getJobRedis())
    task = ClusterTask(1897953162, force=True)
    #1646586724 pingan
    #1897953162 ali
    queue.addTask(task)
    import sys
    sys.exit()
    fd = open("/home/xiafan/KuaiPan/dataset/user/comidbyidx.txt")
    for line in fd.readlines():
        task = ClusterTask(long(line), force=False)
        queue.addTask(task)
    fd.close()
    
if __name__ == "__main__":
    #loadCompTopUsers()
    loadAllCompUsers()