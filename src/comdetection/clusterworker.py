from ConfigParser import ConfigParser
from network import ClusterClient
from network.ttypes import  *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from threading import Thread

from cluster.community import *
from cache.graphcache import GraphCache
from task.taskgen import TaskGen 
import os
from dao.tweetdao import *
from dao.datalayer import DataLayer
from cluster.comdetection import *
from dbinfo import *
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s"
)

class ReportThread(Thread):
    def __init__(self, slaveWorker):
        super(ReportThread,self).__init__()
        self.slaveWorker = slaveWorker
        
    def run(self):
        while self.slaveWorker.working:
            logging.info("start report service")
            processor = ClusterClient.Processor(self)
            transport = TSocket.TServerSocket(port=9090)
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            self.reportServer = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
            self.reportServer.serve()

    def stopReport(self):
        self.reportServer.stop()

class ClusterTaskQueue(object):
    def __init__(self, rCluster):
        self.redis = rCluster.getRedis("1", CLUSTER_JOB_DB)
    
    def nextTask(self):
        tasktypes=['cluster']
        while True:
            hit = False
            for tasktype in tasktypes:
                ret = self.redis.lpop(tasktype)
                if ret:
                    ret = [int(field) for field in ret.split(',')]
                    hit = True
                    yield (tasktype, ret[0], ret[1])
            if not hit:
                sleep(0.1)

class ClusterThread(Thread):
    def __init__(self, worker):
        super(ClusterThread, self).__init__()
        self.worker = worker
        
    def run(self):
        detect = CommDetection(self.worker.dataLayer)
        dao = self.worker.dataLayer.getDBCommInfoDao()
        self.stateDao = self.worker.dataLayer.getClusterStateDao()
        worker = self.worker
        while worker.working:
            for task in worker.taskGen.nextTask():
                logging.info("processing task %s"%(str(task)))
                self.setJobState(task[1], 0, long(time.time()))
    
                ret = detect.detect(task[1], task[2])
                try:
                    dao.updateUserNeighComms(task[1],ret[0])
                    dao.updateGroupTags(task[1], ret[1])
                    self.setJobState(task[1], 1, long(time.time()))
                except:
                    try:
                        self.setJobState(task[1], 2, long(time.time()))
                        dao.close()
                        dao = self.worker.dataLayer.getDBCommInfoDao()
                    except:
                        pass
        dao.close()
                        
    def setJobState(self, uid, state, time):
        try:
            if self.stateDao:
                self.stateDao = self.worker.dataLayer.getClusterStateDao()
            self.stateDao.setClusterState(uid, 2, long(time.time()))
        except:
            try:
                if self.stateDao:
                    self.stateDao.close()
            except:
                self.stateDao = None

class ClusterWorker:
    def __init__(self, config):
        self.config = config
        self.workStatus = WorkStatus()

    def start(self):
        self.working = True
        self.dataLayer= DataLayer(self.config)

        # start status report service
        self.reportThread = ReportThread(self)
        self.reportThread.start()
        
        logging.info("start worker thread")
        self.taskGen = ClusterTaskQueue(self.dataLayer.getJobRedis())        
        tnum = self.config.getint('cluster', 'threadnum')
        self.threads=[]
        for i in range(tnum):
            workThread = ClusterThread(self)
            workThread.start()
            self.threads.append(workThread)
        
        #waiting for shutdown
        
        while len(self.threads) > 0:
            try:
                self.threads[0].join()
                self.threads.pop(0)
            except:
                pass
        logging.info("cluster worker shuts down")
   
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
    cpath = os.path.join(os.getcwd(), "../../conf/dworker.conf")
    print "load config file:", cpath
    config.read(cpath)

    slaveWorker = ClusterWorker(config)
    slaveWorker.start() 
