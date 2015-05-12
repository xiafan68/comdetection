# coding:UTF8
import tweet.Tweet

"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
class FileBasedTweetDao(object):
    def __init__(self, tfile):
        self.tmap = {}
        self.tfile = tfile
    
    def open(self):
        self.fd = open(self.tfile)
        for line in fd.readlines():
            t = Tweet()
            t.parse(line)
            self.tmap[t.mid] = t
    def getTweet(self, mid):
        return self.tmap[mid]
    
    def getTweets(self, mids):
        ret = []
        for mid in mids:
            ret.append(self.getTweet(mid))
    
    def writeTweet(self, tweet):
        if not tweet.mid in self.tmap:
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
            t = Tweet()
            t.parse(line)
            self.tmap[t.mid] = t
    def getTweet(self, mid):
        redis = self.redisCluster.getRedis(tweet.mid)
        t = Tweet()
        for field in t.schema:
            t.setattr(field, redis.hget(tweet.mid, field))
    
    def getTweets(self, mids):
         ret = []
         for mid in mids:
            ret.append(self.getTweet(mid))
    
    def writeTweet(self, tweet):
        redis = self.redisCluster.getRedis(tweet.mid)
        pipe = redis.pipeline(transaction=False)
        for field in Tweet.schema:
            pipe.hset(tweet.mid, field, gettattr(tweet, field))
        pipe.execute()
        
    def close(self):
        pass
