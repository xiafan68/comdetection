__author__ = 'pc'
from random import randint
import logging
from redis import RedisCluster
from network import *
from network.ttypes import  *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import thread
from cluster.community import *

class ReportThread(thread):
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
        
class ClusterThread(thread):
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
        self.workThread = ClusterThread()
        self.workThread.start()
        
    """
    the following function are status report function
    """
    def reportStatus(self):
        return self.workStatus

    def reAssignJob(self, jobQueueID):
        self.workStatus.jobQueueID = jobQueueID

    # stop processing
    def stop(self):
    	self.working = false
        self.workThread.join()
        self.dataCluster.close()
        self.taskCluster.close()
        
        self.reportThread.stopReport()
        self.reportThread.join()

if __name__ == "__main__":
    slaveWorker = SlaveWorker()
    slaveWorker.start() 
