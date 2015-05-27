from xredis.RedisCluster import RedisCluster
from chaineddao import ChainedDao
import MySQLdb 
from dao.tweetdao import *
from dao.userdao import *
import logging
from cache.graphcache import *
from dao.comminfodao import DBCommInfoDao
from dao.clusterstate import *
from redisinfo import *
from cache.graphcache import GraphCache

class DataLayer(object):
    def __init__(self, config):
        logging.info("initialize data access layer")
        self.config = config
        snservers=[server.strip() for server in config.get('sncache','hosts').split(",")]
        snports=[int(port) for port in config.get('sncache', 'ports').split(',')]
        self.snCluster=RedisCluster(zip(snservers, snports))
        self.snCluster.start()
        
        uservers=config.get('profilecache','hosts').split(",")
        uports=[int(port) for port in config.get('profilecache', 'ports').split(',')]
        self.profileCluster=RedisCluster(zip(uservers, uports))    
        self.profileCluster.start()
        
        jobservers=[server.strip() for server in  config.get('jobqueue','hosts').split(",")]
        jobports=[int(port) for port in config.get('jobqueue', 'ports').split(',')]
        self.jobCluster=RedisCluster(zip(jobservers, jobports))
        self.jobCluster.start()
        
    def getSNRedis(self):
        return self.snCluster
    
    def getProfileRedis(self):
        return self.profileCluster
    
    def getJobRedis(self):
        return self.jobCluster
    
    def getGraphCache(self):
        return GraphCache(self.snCluster, self.profileCluster)
    
    def getDBUserDao(self):
        conn = MySQLdb.connect(host=self.config.get('mysql',"host"),
                               port=self.config.getint('mysql', 'port'),
                               user=self.config.get('mysql',"user"),
                               passwd=self.config.get('mysql',"passwd"),
                               db=self.config.get('mysql','db'),
                               charset="utf8")
        dao = DBUserDao(conn)
        return dao
    
    def getCachedTweetDao(self):
        pass
    
    def getDBTweetDao(self):
        conn = MySQLdb.connect(host=self.config.get('mysql',"host"),
                               port=self.config.getint('mysql', 'port'),
                               user=self.config.get('mysql',"user"),
                               passwd=self.config.get('mysql',"passwd"),
                               db=self.config.get('mysql','db'),
                               charset="utf8")
        dao = DBTweetDao(conn)
        return dao
    
    def getDBCommInfoDao(self):
        conn = MySQLdb.connect(host=self.config.get('mysql',"host"),
                               port=self.config.getint('mysql', 'port'),
                               user=self.config.get('mysql',"user"),
                               passwd=self.config.get('mysql',"passwd"),
                               db=self.config.get('mysql','db'),
                               charset="utf8")
        dao = DBCommInfoDao(conn)
        return dao
    
    def getCachedCrawlUserDao(self):
        ret= ChainedDao([RedisUserDao(self.profileCluster), self.getDBUserDao(),
                         UserDataCrawlerDao(WeiboCrawler(TokenManager(self.config)))])#
        return ret
    
    def getCachedCrawlTweetDao(self):
        ret= ChainedDao([TweetCrawlerDao(WeiboCrawler(TokenManager()))])
        return ret
    
    def getClusterStateDao(self):
        conn = MySQLdb.connect(host=self.config.get('mysql',"host"),
                               port=self.config.getint('mysql', 'port'),
                               user=self.config.get('mysql',"user"),
                               passwd=self.config.get('mysql',"passwd"),
                               db=self.config.get('mysql','db'),
                               charset="utf8")
        dao = ClusterStateDao(conn)
        return dao