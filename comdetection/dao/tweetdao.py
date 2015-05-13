# coding:UTF8
from weiboobj import Tweet
import logging

logger = logging.getLogger("tweetdao")
class UserDataCrawler(object):
    def __init__(self, weiboCrawler):
        self.weiboCrawler = weiboCrawler
        
    def getTweet_(self,mid):
        logger.info("crawling tweet %s"%(mid))
        status = self.weiboCrawler.statuses.show.get(id=mid)
        t = Tweet()
        if status:
            for k,v in status.items():
                if k == "user":
                    t.setattr('uid', v['id'])
                elif k == "retweeted_status":
                    t.setattr("rtmid", v['mid'])
                else:
                    t.setattr(k,v)
            return t
        else:
            return None
    
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
class FileBasedTweetDao(UserDataCrawler):
    def __init__(self, tfile,weiboCrawler):
        super(FileBasedTweetDao, self).__init__(weiboCrawler)
        self.tmap = {}
        self.tfile = tfile
    
    def open(self):
        try:
            self.fd = open(self.tfile, "a+")
            for line in self.fd.readlines():
                t = Tweet()
                t.parse(line)
                self.tmap[t.mid] = t
        except Exception, ex:
            print str(ex)
        
    def getTweet(self, mid):
        if not (mid in self.tmap):
            t = None #super(FileBasedTweetDao, self).getTweet_(mid)
            if t:
                self.writeTweet(t)
                self.tmap[t.mid] = t
        return self.tmap.get(mid, None)
    
    def getTweets(self, mids):
        ret = []
        for mid in mids:
            ret.append(self.getTweet(mid))
    
    def writeTweet(self, tweet):
        self.fd.write(str(tweet))
        self.fd.write("\n")
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
        for line in self.fd.readlines():
            t = Tweet()
            t.parse(line)
            self.tmap[t.mid] = t
            
    def getTweet(self, mid):
        redis = self.redisCluster.getRedis(mid)
        t = Tweet()
        for field in t.schema:
            t.setattr(field, redis.hget(mid, field))
    
    def getTweets(self, mids):
         ret = []
         for mid in mids:
            ret.append(self.getTweet(mid))
    
    def writeTweet(self, tweet):
        redis = self.redisCluster.getRedis(tweet.mid)
        pipe = redis.pipeline(transaction=False)
        for field in Tweet.schema:
            pipe.hset(tweet.mid, field, getattr(tweet, field))
        pipe.execute()
        
    def close(self):
        pass

if __name__ == "__main__":
    from weiboobj import *
    from weibo.weibocrawler import *
    c = WeiboCrawler(TokenManager())
    tdao = FileBasedTweetDao("../../tweets.data",c)
    tdao.open()
    #3810070670785704
    t = tdao.getTweet("3725460108519438")
    
    import jieba
    print ",".join(jieba.cut(t.text, cut_all=False))
    print str(t)
   
    tdao.close()