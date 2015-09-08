# coding:utf8
import random
from ConfigParser import ConfigParser
from network import ClusterClient
from network.ttypes import  *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from threading import Thread

from cluster.community import *
from cluster.scan import *
from cache.graphcache import GraphCache
# from task.taskgen import TaskGen 
import os
from dao.tweetdao import *
from dao.datalayer import DataLayer
# from dao.clusterstate import ClusterState
from dao.clusterstate import *
from cluster.summarization import ComSummarize
from redisinfo import *
import time
import cPickle
from task.taskqueue import *
import traceback
import sys
from dao import userdao

import logging
import logging.config
logPath = os.path.join(os.getenv("COMMUNITY_HOME", os.getcwd()), "./conf/logger.conf")
logging.config.fileConfig(logPath)
cslogger = logging.getLogger()

class ReportThread(Thread):
    def __init__(self, slaveWorker):
        super(ReportThread, self).__init__()
        self.slaveWorker = slaveWorker
        
    def run(self):
        while self.slaveWorker.working:
            cslogger.info("start report service")
            processor = ClusterClient.Processor(self)
            transport = TSocket.TServerSocket(port=9090)
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            self.reportServer = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
            self.reportServer.serve()

    def stopReport(self):
        self.reportServer.stop()
    
"""
this is the class that accepts cluster tasks and executes them.
"""
class ClusterThread(Thread):
    def __init__(self, worker):
        super(ClusterThread, self).__init__()
        self.worker = worker
        self.clustergap = worker.config.getint('crawl', 'clustergap')
        self.maxclustertime = worker.config.getint('crawl', 'maxclustertime')
        self.id = random.randint(0, 10000000)
    def run(self):
        dao = self.worker.dataLayer.getDBCommInfoDao()
        self.stateDao = self.worker.dataLayer.getClusterStateDao()
        worker = self.worker
        sumIns = ComSummarize(self.worker.dataLayer.getTagStatsDao())
        while worker.working:
            try:
                task = worker.taskGen.nextTask()
                cslogger.info("processing task %s" % (str(task[1])))
                state = self.stateDao.getClusterState(task[1].uid)

                if not task[1].force:
                    if (state and not state.shouldRerun(self.maxclustertime, self.clustergap)):
                        continue
                                    
                try:
                    curState = ClusterState(task[1].uid, self.id)
                    # a previous job may fail
                    if not self.stateDao.setupLease(curState, state):
                        continue
                    cret = self.execGraphCluster(task[1])
                    if self.stateDao.extendLease(curState):
                        cslogger.info("compute summarization communities")
                        ret = sumIns.summarize(cret[0], cret[1])
                        sumIns.close()
                        if self.stateDao.extendLease(curState):
                            cslogger.info("store neighbours communities")
                            dao.updateUserNeighComms(task[1].uid, ret[0])
                            cslogger.info("store  communities tags")
                            dao.updateCommTags(task[1].uid, ret[1])
                            curState.state = CLUSTER_SUCC
                            self.stateDao.setClusterState(curState)
                            cslogger.info("task completes")
                except Exception as ex:
                    curState.state = CLUSTER_FAIL
                    self.stateDao.setClusterState(curState)
                    print str(ex)
                    try:
                        dao.close()
                        dao = self.worker.dataLayer.getDBCommInfoDao()
                    except:
                        pass
            except Exception as ex:
                cslogger.error(ex)
                exc_type, exc_value, exc_tb = sys.exc_info()
                for filename, linenum, funcname, source in traceback.extract_tb(exc_tb):
                    print "%-23s:%s '%s' in %s()" % (filename, linenum, source, funcname)
        dao.close()
    
    def execGraphCluster(self, task):
        gcache = self.worker.dataLayer.getGraphCache()
        ego = gcache.egoNetwork(task.uid)
        # gcache.loadProfiles(ego)
        gcache.loadTags(ego)
        gcache.close()
        
        #update edge weight
        for a in ego.nodes():
            for b in ego.nodes():
                tagsa = set(ego.tags.get(a, []))
                tagsb = set(ego.tags.get(b, []))
                if tagsa and tagsb:
                    sim = len(tagsa & tagsb) / float(len(tagsa | tagsb))
                    if sim > 0.3:
                        ego.addEdge(ego.getEdgeNum(), a, b, 1.0)
        """
        for edge in ego.getEdges():
            tagsa = set(ego.tags.get(edge[0], []))
            tagsb = set(ego.tags.get(edge[1], []))
            if tagsa and tagsb:
                sim = len(tagsa & tagsb) / float(len(tagsa | tagsb)) + edge[3]
                ego.udpateEdgeWeight(sim)
        """
        
        #comm = Community(ego, 0.01, task.cnum, 3)
        #comm.initCommunity()
        #comm.startCluster()
        #comm.close()
        comm = scan(ego)
        comm.computeCommunity()
        return (ego, comm.n2c)
    
    def summarize(self, comm):
        return sum.summarize(comm[0], comm[1])
    def setJobState(self, uid, state, time):
        try:
            if not self.stateDao:
                self.stateDao = self.worker.dataLayer.getClusterStateDao()
            self.stateDao.setClusterState(uid, state, time)
        except Exception as ex:
            print str(ex)
            try:
                if self.stateDao:
                    self.stateDao.close()
                    self.stateDao = None
            except:
                self.stateDao = None

class ClusterWorker:
    def __init__(self, config):
        self.config = config
        self.workStatus = WorkStatus()
        try:
            self.id = config.getint('workder', 'id')
        except:
            self.id = random.randint(0, 10000000)
        
    def start(self):
        self.working = True
        self.dataLayer = DataLayer(self.config)

        # start status report service
        self.reportThread = ReportThread(self)
        self.reportThread.start()
                
        self.taskGen = ClusterTaskQueue(self.dataLayer.getJobRedis())        
        tnum = self.config.getint('cluster', 'threadnum')
        cslogger.info("start %d worker threads" % (tnum))
        self.threads = []
        for i in range(tnum):
            workThread = ClusterThread(self)
            workThread.start()
            self.threads.append(workThread)
        
        # waiting for shutdown
        
        while len(self.threads) > 0:
            try:
                self.threads[0].join()
                self.threads.pop(0)
            except:
                pass
        cslogger.info("cluster worker shuts down")
   
    """
    the following function are status report function
    """
    def reportStatus(self):
        return self.workStatus

    def reAssignJob(self, jobQueueID):
        self.workStatus.jobQueueID = jobQueueID

    def clusterForNode(self, nodeID):
        self.taskGen.addNewTask(nodeID)
        
    # stop processing
    def stop(self):
        self.working = False
        self.workThread.join()
        self.dataCluster.close()
        self.taskCluster.close()
        
        self.reportThread.stopReport()
        self.reportThread.join()

if __name__ == "__main__":
    
    config = ConfigParser()
    cpath = os.path.join(os.getenv("COMMUNITY_HOME", os.getcwd()), "./conf/dworker.conf")
    print "load config file:", cpath
    config.read(cpath)

    
    slaveWorker = ClusterWorker(config)
    slaveWorker.start() 
