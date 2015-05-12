#coding:UTF8
from xredis.RedisCluster import RedisCluster
from dao.userdao import FileBasedUserDao
from dao.tweetdao import FileBasedTweetDao
from cache.graphcache import GraphCache 
from dao.weiboobj import *
from weibo.weibocrawler import *
from cluster.community import Community
import jieba

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s"
)

class CommDetection:
    def __init__(self, udao, tdao, gcache):
        self.udao = udao
        self.tdao = tdao
        self.gcache = gcache
    
    def detect(self, uid):
        ego = self.gcache.egoNetwork(uid)
        
        comm = Community(ego, 0.01, 10, 2)
        comm.initCommunity()
        comm.startCluster()
        
        comms = {}
        for k,v in comm.n2c.items():
            if v not in comms:
                comms[v]=[]
            comms[v].append(k)
            
        self.summarize(comms)
    #choose tags for each community
    def summarize(self, comms):
        print "begin to summarize"
        for k, v in comms.items():
            print "%s\n%s"%(str(k), str(self.buildTweetWordCloud(v)))
            
    def buildTweetWordCloud(self, v):
        wordHist={}
        for uid in v:
            mids = udao.getMids(uid)
            for mid in mids:
               t = tdao.getTweet(mid)
               if t:
                   for word in jieba.cut(t.text, cut_all=False):
                       wordHist[word] = wordHist.get(word, 1) + 1
        return wordHist
    
if __name__ == "__main__":
    snRedis=RedisCluster([ ("10.11.1.51", 6379),
            ("10.11.1.52", 6379), ("10.11.1.53", 6379), ("10.11.1.54", 6379), ("10.11.1.55", 6379),
           ("10.11.1.56", 6379), ("10.11.1.57", 6379), ("10.11.1.58", 6379), ("10.11.1.61", 6379),
            ("10.11.1.62", 6379), ("10.11.1.63", 6379)], 0)
    snRedis.start()

    profileRedis=RedisCluster([("10.11.1.64",6379)],1)
    profileRedis.start()
    

    c = WeiboCrawler(TokenManager())
    udao = FileBasedUserDao("../ups.txt","../umids.txt",c)
    udao.open()
    tdao = FileBasedTweetDao("../tweets.data",c)
    tdao.open()
    
    gcache = GraphCache(snRedis, profileRedis)
    com = CommDetection(udao, tdao, gcache)
    com.detect("1707446764")
    
    udao.close()
    tdao.close()
    
    #"1707446764"