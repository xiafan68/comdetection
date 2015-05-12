#coding:UTF8
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""

from weiboobj import *
from weibo.weibocrawler import *

class UserDataCrawler(object):
    def __init__(self, weiboCrawler):
        self.weiboCrawler = weiboCrawler
        
    def getUserProfile_(self,uid):
        user = self.weiboCrawler.users.show.get(uid=uid)
        up = UserProfile()
        for k,v in user.items():
            up.setattr(k,v)
        return up
    
    def getMids_(self, uid):
        jsonRet = self.weiboCrawler.statuses.user_timeline.ids.get(uid=uid)
        if jsonRet:
            return jsonRet['statuses']
        else:
            return []
        
class FileBasedUserDao(UserDataCrawler):
    def __init__(self, ufile, uTweetsFile, weiboCrawler):
        super(FileBasedUserDao, self).__init__(weiboCrawler)
        self.umap={}
        self.uMids={}
        self.tfile = ufile
        self.uTweetsFile = uTweetsFile
        
    def open(self):
        try:
            self.fd = open(self.tfile, "a+")
            for line in self.fd.readlines():
                if not line:
                    break
                t = UserProfile()
                t.parse(line)
                self.umap[t.id]=t
        except Exception, ex:
            print str(ex)
        
        try:
            self.utfd = open(self.uTweetsFile, "a+")
            for line in self.utfd.readlines():
                if not line:
                    break
                fields = line.split("\t")
                mids = fields[1].split(",")
                self.tMids[fields[0]] = mids
        except Exception, ex:
            print str(ex)
            
    def getUserProfile(self, uid):
        if not(uid in self.umap):
            up = self.getUserProfile_(uid)
            if up:
                self.writeUserProfile(up)
                self.umap[str(up.id)]=up
        return self.umap[uid]
    
    def getUserProfiles(self, uids):
        ret=[]
        for uid in uids:
            ret.append(self.getUserProfile(uid))
    
    def getMids(self, uid):
        if not( uid in self.uMids):
            mids = super(FileBasedUserDao, self).getMids_(uid)
            if mids:
                self.utfd.write("%s\t%s"%(uid, ",".join(mids)))
                self.utfd.write("\n")
                self.uMids[uid]= mids
        return self.uMids.get(uid, [])
        
    def writeUserProfile(self, user):
        self.fd.write(str(user))
        self.fd.write("\n")
        self.fd.flush()
    
    def close(self):
        self.fd.close()
        self.utfd.close()
        
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
class RedisUserDao(UserDataCrawler):
    def __init__(self, redisCluster, weibocrawler):
        self.redisCluster = redisCluster
        self.weibocrawler = weibocrawler
    
    def open(self):
        self.fd = open(self.tfile)
        for line in self.fd.readlines():
            if not line:
                break
            
            t = Tweet()
            t.parse(line)
            self.tmap[t.mid]=t
    
    def getUser(self, uid):
        redis = self.redisCluster.getRedis(uid)
        u = UserProfile()
        fields = redis.hgetall(uid)
        if fields:
            for field in u.schema:
                u.setattr(field, fields[field])
        else:
            up = self.getUserProfile_(uid)
            self.writeUser(up)
        return u
    
    def getUsers(self, uids):
        ret=[]
        for uid in uids:
           ret.append(self.getUser(uid))
        return ret
    
    def writeUser(self, user):
        redis = self.redisCluster.getRedis(user.id)
        pipe = redis.pipeline(transaction=False)
        for field in user.schema:
            pipe.hset(user.id, field, getattr(user, field))
        pipe.execute()
        
    def close(self):
        pass
    

if __name__ == "__main__":
    c = WeiboCrawler(TokenManager())
    tdao = FileBasedUserDao("../../ups.data", "../../umids.txt",c)
    tdao.open()
    up = tdao.getUserProfile("1707446764")
    import jieba
    print ",".join(jieba.cut(up.name, cut_all=False))
   
    print str(tdao.getMids("1707446764"))
    tdao.close()