# coding:UTF8
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""

from weiboobj import *
from weibo.weibocrawler import *
import logging
import os
from redisinfo import *

logger = logging.getLogger()
class UserDataCrawlerDao(object):
    def __init__(self, weiboCrawler, tagDao):
        self.weiboCrawler = weiboCrawler
        self.tagDao = tagDao
        
    def getUserProfile(self, uid):
        logger.info("crawling profile for %s"%(uid))
        user = self.weiboCrawler.users.show.get(uid=uid)
        up = UserProfile()
        if user:
            for k, v in user.items():
                up.setattr(k, v)
            logger.info("get user %s"%(up.name))
            up.setattr('uid',uid)
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
                        if self.tagDao:
                            self.tagDao.updateTagCount(1, v)  
        return ret    

    def getUserFriendsID(self, uid):
        logger.info("crawling friends of %s"%(uid))
        ret = []
        jsonRet = self.weiboCrawler.friendships.friends.ids.get(uid=uid)
        if jsonRet:
            ret = jsonRet['ids']
        return ret
   
    def close(self):
        pass
    
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
                self.umap[t.uid] = t
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
import time
class DBUserDao(object):
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
        try:
            user.lastcrawltime=long(time.time())
            user.crawlstate=2
            sql ="insert into userprofile(%s) values(%s) on duplicate key update %s;" % ( user.getSQLColums(), user.getSQLValues(), user.getUpdate("uid"))
            #print sql
            cursor.execute(sql)
        except Exception as ex:
            print str(ex)
        finally:
            cursor.close()
            self.conn.commit()

    def getUserMids(self, uid):
        return self.uMids.get(uid, [])
    
    def updateUserMids(self, mids, uid):
        cursor = self.conn.cursor()
        midstr = ",".join(mids)
        cursor.execute("insert into usermids(uid, mids) values('%s', '%s') on duplicate key update tags='%s';" % (uid, midstr, midstr))
        cursor.close()
        self.conn.commit()
        
    def getUserTags(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute("select tags from usertags where uid='%s';" % (uid))
        res = cursor.fetchall()
        
        ret = []
        for rec in res:
            ret = rec['tags'].split(",")
            break
        cursor.close()
        return ret
    
    def updateUserTags(self, utags, uid):
        cursor = self.conn.cursor()
        tagstr = ",".join(utags)
        sql = "insert into usertags(uid, tags) values('%s', '%s') on duplicate key update tags='%s';" % (uid, tagstr, tagstr)
        cursor.execute(sql)
        cursor.close()
        self.conn.commit()
    
    def updateUserFriendsID(self, uid):
        pass
        
    def close(self):
        self.conn.close()
"""
用于访问tweets数据的类,这个类只能从文件中将数据全部导入内存
"""
class RedisUserDao(object):
    def __init__(self, redisCluster):
        self.redisCluster = redisCluster
    
    def open(self):
        pass
    
    def getUserProfile(self, uid):
        redis = self.redisCluster.getRedis(uid, PROFILE_DB)
        fields = redis.hgetall(uid)
        if fields:
            u = UserProfile()
            for field in u.schema:
                if field in fields:
                    u.setattr(field, fields[field])
            return u
        else:
            return None
        
    def getUsers(self, uids):
        ret = []
        for uid in uids:
            ret.append(self.getUser(uid))
        return ret
    
    def updateUserProfile(self, user,uid):
        redis = self.redisCluster.getRedis(uid,PROFILE_DB)
        pipe = redis.pipeline(transaction=False)
        for field in user.schema:
            pipe.hset(uid, field, getattr(user, field))
        pipe.execute()
        
    def getUserTags(self, uid):
        redis = self.redisCluster.getRedis(uid,USER_TAGS_DB)
        tags = redis.get(uid)
        ret=[]
        if tags:
            ret = tags.split(",")
        return ret
    
    def updateUserTags(self, utags, uid):
        redis = self.redisCluster.getRedis(uid,USER_TAGS_DB)
        redis.set(uid, ",".join(utags))
    
    def getUserMids(self):
        pass
    
    def updateUserMids(self):
        pass
    
    def close(self):
        pass
    
class RedisSocialDao(object):
    def __init__(self, snCluster):
        self.snCluster = snCluster
        
    def getUserFriendsID(self, uid):
        redis= self.snCluster.getRedis(uid, SN_DB)
        return redis.smembers(uid) 
    
    def updateUserFriendsID(self,friends, uid):
        redis = self.snCluster.getRedis(uid,SN_DB)
        pipe = redis.pipeline(transaction=False)
        for fid in friends:
            pipe.sadd(uid, fid)
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
