#coding:UTF8
from threading import Thread
import time.sleep as sleep
from dao.chaineddao import ChainedDao
from dao.datalayer import *
"""
用于读取爬去任务,目前基于redis的list实现
"""

class TaskQueue(object):
    def __init__(self, rCluster):
        self.redis = rCluster.getRedis("1")
    
    def nextTask(self):
        tasktypes=['tag', 'profile', 'mids', 'tweet', 'friends']
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
        self.server = server
        
        udao = ChainedDao([server.datalayer.getDBUserDao(), UserDataCrawlerDao(WeiboCrawler(TokenManager()))])
        tdao = ChainedDao([server.datalayer.getDBTweetDao(), UserDataCrawlerDao(WeiboCrawler(TokenManager()))])
        
        self.taskFuncMap={'tag':udao.getUserTags,"mids":udao.getUserMids,
                           'profile':udao.getUserProfile,"tweet": tdao.getTweet}
        
    def run(self):
        while self.server.running:
            task = self.server.taskqueue.nextTask()
            
        
class CrawlerServer(object):
    def __init__(self, config):
        self.datalayer = DataLayer(config)
        self.taskqueue= TaskQueue(self.datalayer.getJobRedis())

