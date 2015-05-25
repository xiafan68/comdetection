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
            hit = False
            for tasktype in tasktypes:
                ret = self.redis.lpop(tasktype)
                if ret:
                    ret =cPickle.loads(ret)
                    hit = True
                    yield (tasktype, ret)
            if not hit:
                sleep(0.1)


if __name__ == "__main__":
    config = ConfigParser()
    cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
    config.read(cpath)
    datalayer = DataLayer(config)
    queue = ClusterTaskQueue(datalayer.getJobRedis())
    task = ClusterTask(1707446764, force=True)
    queue.addTask(task)