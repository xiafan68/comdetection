__author__ = 'xiafan68@gmail.com'

class TaskGen:
    def __init__(self, taskClient, graphCache):
        self.redisClient = taskClient
        self.graphCache = graphCache
        self.redisClient.select(1) #1 is used to store jobs
        self.IDQueue=[]
        self.taskID = 0
        self.recentProcessedIDs=set()

    def nextTask(self):
        id = -1
        if len(self.IDQueue) == 0:
            id = self.IDQueue.append(id = self.redisClient.getRedis(self.taskID).spop(10))
        
	    if len(self.IDQueue.pop()) == 0:
	    	return id
	 
            
        
    def taskFinish(self):

    """
    read the next node id to compute
    """
    def readNextID(self):

