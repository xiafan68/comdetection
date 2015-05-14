# coding:UTF8
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""

from weiboobj import *
from weibo.weibocrawler import *
import os

class UserDataCrawlerDao(object):
    def __init__(self, weiboCrawler):
        self.weiboCrawler = weiboCrawler
        
    def getUserProfile(self, uid):
        user = self.weiboCrawler.users.show.get(uid=uid)
        up = UserProfile()
        if user:
            for k, v in user.items():
                up.setattr(k, v)
            return up
        else:
            return None
        
    def getUserMids(self, uid):
        jsonRet = self.weiboCrawler.statuses.user_timeline.ids.get(uid=uid)
        if jsonRet:
            return jsonRet['statuses']
        else:
            return []    
     
    
    def getUserTags(self, uid):
        ret = []
        jsonRet = self.weiboCrawler.tags.get(uid=uid)
        if jsonRet:
            for tagTuple in jsonRet:
                for (k, v) in tagTuple.items():
                    if k != 'flag' and k != 'weight':
                        ret.append(v)  
        return ret    

class FileBasedUserDao(object):
    def __init__(self, dataDir):
        self.umap = {}
        self.uMids = {}
        self.uTags = {}
        self.dataDir = dataDir
        
    def open(self):
        self.loadProfiles()
        self.loadUserMids()
        self.loadUserTags()
    
    def loadUserTags(self):
        try:
            tfile = os.path.join(self.dataDir, "utags.data")
            self.tagfd = open(tfile, "a+")
            for line in self.tagfd.readlines():
                if not line:
                    break
                fields = line.split("\t")
                uid = fields[0].decode('utf8')
                fields = [field.decode('utf8') for field in fields[1:-1]]
                self.uTags[uid] = fields
        except Exception, ex:
            print str(ex)
    """
    加载用户的Profile
    """
    def loadProfiles(self):
        try:
            tfile = os.path.join(self.dataDir, "ups.data")
            self.fd = open(tfile, "a+")
            for line in self.fd.readlines():
                if not line:
                    break
                t = UserProfile()
                t.parse(line)
                self.umap[t.id] = t
        except Exception, ex:
            print str(ex)
    """
    加载用户发布的微博ids
    """
    def loadUserMids(self):    
        try:
            uTweetsFile = os.path.join(self.dataDir, "umids.data")
            self.utfd = open(uTweetsFile, "a+")
            for line in self.utfd.readlines():
                if not line:
                    break
                fields = line.split("\t")
                mids = fields[1].split(",")
                self.uMids[fields[0]] = mids
        except Exception, ex:
            print str(ex)
            
    def getUserProfile(self, uid):
        return self.umap.get(uid, None)
    
    def getUserProfiles(self, uids):
        ret = []
        for uid in uids:
            ret.append(self.getUserProfile(uid))
    
    def getUserMids(self, uid):
        return self.uMids.get(uid, [])
    
    def getUserTags(self, uid):
        return self.uTags.get(uid, [])
    
    def updateUserMids(self, mids, uid):
        self.utfd.write("%s\t%s" % (uid, ",".join(mids)))
        self.utfd.write("\n")
        self.utfd.flush()
        self.uMids[uid] = mids

    def updateUserTags(self, utags, uid):
        tstr = "\t".join(utags)
        rec = "%s\t%s\n" % (uid, tstr)
        self.tagfd.write(rec.encode('utf8'))
        self.tagfd.flush()
        self.uTags[uid] = utags
                
    def updateUserProfile(self, user, uid):
        self.fd.write(str(user))
        self.fd.write("\n")
        self.fd.flush()
        self.umap[str(up.id)] = up
    
    def close(self):
        self.fd.close()
        self.utfd.close()
        self.tagfd.close()

import MySQLdb
class MysqlUserDao(object):

    def __init__(self, conn):
        self.conn = conn
        
    def open(self):
        pass
    
    def getUserProfile(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute("select * from userprofile where uid='%s';" % (uid))
        res = cursor.fetchall()
        
        ret = None
        for rec in res:
            ret = UserProfile()
            for (k, v) in rec.items():
                ret.setattr(k, v) 
            break
        cursor.close()
        return ret
    
    def updateUserProfile(self, user, uid):
        cursor = self.conn.cursor()
        cursor.execute("insert into userprofile(%s) values(%s) on duplicate key update %s where uid=%s;" % (user.getSQLColums(), user.getSQLValues(), user.getUpdate("uid"), uid))
        cursor.close()
    
    def getUserMids(self, uid):
        return self.uMids.get(uid, [])
    
    def updateUserMids(self, mids, uid):
        cursor = self.conn.cursor()
        midstr = ",".join(mids)
        cursor.execute("insert into usermids(uid, tags) values(%s, '%s') on duplicate key update tags='%s' where uid=%s;" % (uid, midstr, midstr,uid))
        cursor.close()

    def getUserTags(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute("select * from usertags where uid='%s';" % (uid))
        res = cursor.fetchall()
        
        ret = []
        for rec in res:
            for (k, v) in rec.items():
                ret = v.split(",")
            break
        cursor.close()
        return ret
    
    def updateUserTags(self, utags, uid):
        tstr = "\t".join(utags)
        rec = "%s\t%s\n" % (uid, tstr)
        self.tagfd.write(rec.encode('utf8'))
        self.tagfd.flush()
        self.uTags[uid] = utags
                
    
    
    def close(self):
        self.fd.close()
        self.utfd.close()
        self.tagfd.close()
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
            self.tmap[t.mid] = t
    
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
        ret = []
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
    tdao = FileBasedUserDao("../../ups.data", "../../umids.txt", c)
    tdao.open()
    up = tdao.getUserProfile("1707446764")
    import jieba
    print ",".join(jieba.cut(up.name, cut_all=False))
   
    print str(tdao.getMids("1707446764"))
    tdao.close()
