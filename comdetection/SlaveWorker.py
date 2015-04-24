__author__ = 'pc'
from ConfigParser import ConfigParser
from random import randint
import logging
from redis import RedisCluster
from network import *
from network.ttypes import  *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from threading import Thread

from util.config import Config
from cluster.community import *
from cache.graphcache import GraphCache
from task.taskgen import TaskGen 

class ReportThread(Thread):
    def __init__(self, slaveWorker):
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
        
class ClusterThread(Thread):
	def __init__(self, taskGen):
		self.taskGen = taskGen
		super(ClusterThread, self).__init__()
		
	def run(self, slaveWorker):
		while slaveWorker.working:
			task = self.taskGen.nextTask()
			if task:
				comm = Community(task[1], 0.01, 10, 2)
    			comm.initCommunity()
    			comm.startCluster()
                for neighbour in task[1].neighbours(task[0]) :
                    #TODO store the cluster results
                    comm.getComm(neighbour)
                self.taskGen.taskFinish(task[0])
                
class SlaveWorker:
    def __init__(self, config):
        self.dataServers = config['dataServers']
        self.taskServers = config['taskServers']
        self.workStatus = WorkStatus()
        self.workStatus.jobQueueID = randint() % config['JOB_QUEUE_SIZE']

    def start(self):
    	self.working = true
        # start redis cluster
        logging.info("start redis cluster")
        self.dataCluster = RedisCluster(self.dataServers, 0)
        self.dataCluster.start()
        self.taskCluster = RedisCluster(self.taskServers, 1)
        self.taskCluster.start()

        # start status report service

        self.reportThread = ReportThread()
        self.reportThread.start()
        
        logging.info("start worker thread")
        self.taskGen = TaskGen(self.taskCluster, GraphCache(self.dataCluster))
        
        self.workThread = ClusterThread(taskGen)
        self.workThread.start()

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
    	self.working = false
        self.workThread.join()
        self.dataCluster.close()
        self.taskCluster.close()
        
        self.reportThread.stopReport()
        self.reportThread.join()

if __name__ == "__main__":
    Config.
    slaveWorker = SlaveWorker(Config.config)
    slaveWorker.start() 
