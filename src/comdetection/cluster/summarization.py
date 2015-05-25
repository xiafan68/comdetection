#coding:UTF8
from xredis.RedisCluster import RedisCluster
from cache.graphcache import GraphCache 
from dao.weiboobj import *
from dao.userdao import *
from dao.tweetdao import *
from weibo.weibocrawler import *
from cluster.community import Community
import jieba
from threading import Thread

logging.basicConfig(
    level=logging.ERROR,
    format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s"
)

class ComSummarize(object):
    def __init__(self, datalayer):
        self.udao = datalayer.getCachedCrawlUserDao()
      
    #choose tags for each community
    def summarize(self, ego, n2c):
        comms = {}
        for k,v in n2c.items():
            if v not in comms:
                comms[v]=[]
            comms[v].append(k)
            
        self.globalstats={}
        self.groupstats={}
        self.stats={}
        self.groupRepr={}
        print "begin to summarize"
        for k, v in comms.items():
            self.buildUserTagWordCloud(ego, k,v)
        
        groups={}
        for group, uids in comms.items():
            newGroup = self.groupRepr[group]
            for uid in uids:
                groups[uid] = newGroup
                
        groupTags={}
        for group, v in self.groupstats.items():
            #up = self.udao.getUserProfile(self.groupRepr[group])
            #if up:
            #    group = up.name
            hist=[]
            for (k,v) in v.items():
                    hist.append((k, float(v)/self.globalstats[k]))
            hist = sorted(hist, lambda x,y: cmp(x[1], y[1]), reverse=True)
            groupTags[self.groupRepr[group]]=[tag[0] for tag in hist[0:20]]
            
        return (groups, groupTags)
    
    def buildUserTagWordCloud(self, ego, k, v):
        wordHist={}
        self.groupstats[k] = wordHist
        
        reprNode=[None,-1]
        
        for uid in v:
            tags = self.udao.getUserTags(uid)
            weight = ego.neighWeight(uid)
            if weight > reprNode[1]:
                reprNode[0]=uid
                reprNode[1]=weight
            if not tags:
                continue
            uwordSet=set()
            self.stats['people']=self.stats.get('people',1)+1
            if tags:
                self.stats['has_descr']=self.stats.get('has_descr',1)+1
                
            for word in tags:
                self.globalstats[word]=max(self.globalstats.get(word,0), weight)
                if not word in uwordSet:
                    wordHist[word] = wordHist.get(word, 1) + weight
                uwordSet.add(word)
        self.groupstats[k] = wordHist
        self.groupRepr[k]=reprNode[0]
        
    def buildProfileWordCloud(self,k, v):
        wordHist={}
        self.groupstats[k] = wordHist
        for uid in v:
            up = udao.getUserProfile(uid)
            if not up:
                continue
            uwordSet=set()
            self.stats['people']=self.stats.get('people',1)+1
            if up.descr:
                self.stats['has_descr']=self.stats.get('has_descr',1)+1
                
            for word in jieba.cut(up.descr, cut_all=False):
                if not word in uwordSet:
                    self.globalstats[word]=self.globalstats.get(word,1) + 1
                    wordHist[word] = wordHist.get(word, 1) + 1
                uwordSet.add(word)
        self.groupstats[k] = wordHist
        
    def buildTweetWordCloud(self,k, v):
        wordHist={}
        self.groupstats[k] = wordHist
        for uid in v:
            mids = udao.getMids(uid)
            uwordSet=set()
            for mid in mids:
                t = tdao.getTweet(mid)
                if t:
                    for word in jieba.cut(t.text, cut_all=False):
                        if not word in uwordSet:
                            self.globalstats[word]=self.globalstats.get(word,1) + 1
                            wordHist[word] = wordHist.get(word, 1) + 1
                        uwordSet.add(word)
                        
        self.groupstats[k] = wordHist              
    
if __name__ == "__main__":
    snRedis=RedisCluster([ ("10.11.1.51", 6379),
            ("10.11.1.52", 6379), ("10.11.1.53", 6379), ("10.11.1.54", 6379), ("10.11.1.55", 6379),
           ("10.11.1.56", 6379), ("10.11.1.57", 6379), ("10.11.1.58", 6379), ("10.11.1.61", 6379),
            ("10.11.1.62", 6379), ("10.11.1.63", 6379)], 0)
    snRedis.start()

    profileRedis=RedisCluster([("10.11.1.64",6379)],1)
    profileRedis.start()
    

    c = WeiboCrawler(TokenManager())
    udao = FileBasedUserDao("../../")
    udao.open()
    tdao = FileBasedTweetDao("../../tweets.data")
    tdao.open()
    
    
    com = ComSummarize(udao, tdao, gcache)
    try:
        com.detect("1707446764")
        #com.detect("1650507560")
    finally:
        udao.close()
        tdao.close()
    
    #"1707446764"