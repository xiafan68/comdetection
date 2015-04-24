__author__ = 'xiafan68@gmail.com'
from random import *
from cluster.graph import Graph
from Queue import Queue

"""
 hopes to utilize the local graph cache to avoids read remote data, however, there 
 exists potential risk that workers are computing for the same groups of nodes
"""
class TaskGen:
    def __init__(self, taskClient, graphCache):
        self.taskClient = taskClient
        self.taskClient.select(1)  # 1 is used to store jobs
                
        self.graphCache = graphCache
        
        self.IDQueue = Queue()
        self.localJobQueue = [] 

        self.taskID = 0
        self.recentProcessedIDs = set()

    def addNewTask(self, nodeID):
        self.IDQueue.put(nodeID)
        
    def nextTask(self):
        id = -1
        while True:
            if len(self.localJobQueue) > 0 and random() < 0.7:
                id = self.localJobQueue.pop()
                
            elif len(self.IDQueue) == 0:
                self.IDQueue.append(id=self.taskClient.getRedis(self.taskID).spop(100))
                if len(self.IDQueue) != 0:
    	           id = self.IDQueue.get()
            
            if not (id in self.recentProcessedIDs):
                break
            else:
                id = -1
        if id == -1:
            return [id, Graph()]        
        return [id, self.graphCache.egoNetwork(id)]
	 
    """
    status is true: this node success to compute and store the results
    """
    def taskFinish(self, nodeID, status):
        self.recentProcessedIDs.add(nodeID)
        if len(self.recentProcessedIDs) > 10000:
            self.recentProcessedIDs.pop()
        
        if status:
            self.localJobQueue.extend(self.graphCache.neighbors(nodeID))
            while len(self.localJobQueue) > 10000:
                self.localJobQueue.pop()
        else:
            #remove some nodes from local queue as they may have been computed
            size = randrange(1, 100)
            size = min([size, len(self.localJobQueue)])
            del self.localJobQueue[0:size]
