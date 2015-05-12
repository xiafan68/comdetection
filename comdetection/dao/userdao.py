#coding:UTF8
from weiboobj import *

"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
from Base.TeX.Text import line
class FileBasedUserDao(object):
    def __init__(self, tfile):
        self.umap={}
        self.tfile = tfile
    
    def open(self):
        self.fd = open(self.tfile)
        for line in fd.readlines():
            if not line:
                break
            t = User()
            t.parse(line)
            self.umap[t.mid]=t
    def getTweet(self, mid):
        return self.umap[mid]
    
    def getTweets(self, mids):
        ret=[]
        for mid in mids:
            ret.append(self.getTweet(mid))
    
    def writeTweet(self, tweet):
        if not tweet.mid in self.umap:
            self.fd.write(str(tweet))
            self.fd.flush()
    
    def close(self):
        self.fd.close()
        
        
        
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
class RedisTweetDao(object):
    def __init__(self, redisCluster):
        self.redisCluster
    
    def open(self):
        self.fd = open(self.tfile)
        for line in fd.readlines():
            if not line:
                break
            
            t = Tweet()
            t.parse(line)
            self.tmap[t.mid]=t
    def getUser(self, mid):
        redis = self.redisCluster.getRedis(tweet.mid)
        t = Tweet()
        for field in Tweet.schema:
            t.setattr(field, redis.hget(tweet.mid, field))
    
    def getUsers(self, mids):
       ret=[]
       for mid in mids:
           ret.append(self.getUser(mid))
    
    def writeTweet(self, tweet):
        redis = self.redisCluster.getRedis(tweet.mid)
        pipe = redis.pipeline(transaction=False)
        for field in Tweet.schema:
            pipe.hset(tweet.mid, field, gettattr(tweet, field))
        pipe.execute()
        
    def close(self):
        pass