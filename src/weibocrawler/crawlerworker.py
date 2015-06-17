#coding:UTF8
from threading import Thread
from time import  sleep
from dao.chaineddao import ChainedDao
from dao.datalayer import *
from redisinfo import CLUSTER_JOB_DB
#import thread

"""
用于读取爬去任务,目前基于redis的list实现
"""

class CrawlerTaskQueue(object):
    def __init__(self, rCluster):
        self.redis = rCluster.getRedis("1",CLUSTER_JOB_DB)
        #self.mylock = thread.allocate_lock()
    def nextTask(self):
        tasktypes=['tag', 'profile','egoprofile', 'mids', 'tweet', 'friends']
        while True:
            hit = False
            for tasktype in tasktypes:
                ret = self.redis.lpop(tasktype)
                if ret:
                    hit = True
                    yield (tasktype, ret)
            if not hit:
                sleep(0.1)
            
class CrawlerWorker(Thread):
    def __init__(self, server):
        super(CrawlerWorker,self).__init__()
        self.server = server
        self.graphCache = server.datalayer.getGraphCache()
        
        udao = server.datalayer.getCachedCrawlUserDao()
        tdao = server.datalayer.getCachedCrawlTweetDao()
        
        self.taskFuncMap={'tag':udao.getUserTags,"mids":udao.getUserMids,
                           'profile':udao.getUserProfile,"tweet": tdao.getTweet, "egoprofile":self.egoProfile}
    
    def egoProfile(self, uid):
        graph = self.graphCache.egoNetwork(uid)
        self.graphCache.loadProfiles(graph) 
           
    def run(self):
        while self.server.running:
            for task in self.server.taskqueue.nextTask():
                self.taskFuncMap[task[0]](task[1])
        
class CrawlerServer(object):
    def __init__(self, config):
        self.datalayer = DataLayer(config)
        self.taskqueue= CrawlerTaskQueue(self.datalayer.getJobRedis())

    def start(self):
        self.running = True
        self.threads=[]
        for i in range(5):
            workThread = CrawlerWorker(self)
            workThread.start()
            self.threads.append(workThread)
        
        #waiting for shutdown
        while len(self.threads) > 0:
            try:
                self.threads[0].join()
                self.threads.pop(0)
            except:
                pass
if __name__ == "__main__":
    from ConfigParser import ConfigParser
    config = ConfigParser()
    cpath = os.path.join(os.getcwd(), "../../conf/dworker.conf")
    print "load config file:", cpath
    config.read(cpath)

    slaveWorker = CrawlerServer(config)
    slaveWorker.start() 