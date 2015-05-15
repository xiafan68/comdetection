# coding:UTF8

"""
store community infos
每个用户都在两张表中分别存储了一下信息：
1. 它好友对应的分组信息
2. 每个分组对应的tags
"""
import MySQLdb
import logging
from xredis.RedisCluster import RedisCluster
from dbinfo import *

"""
基于db的dao
"""
class DBCommInfoDao(object):
    def __init__(self, conn):
        self.conn = conn
        
    def open(self):
        pass
    
    def getUserNeighComms(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute("select uid, friid, group from neighgroups where uid='%s';" % (uid))
        res = cursor.fetchall()
        
        ret = {}
        for rec in res:
            ret[rec['friid']] = rec['group']
        cursor.close()

        return ret
    
    def updateUserNeighComms(self, uid, groups):
        cursor = self.conn.cursor()
        sql = "insert into neighgroups(uid, friid, group) " 
        "values(%s,%s,%s) on duplicate key update "
        "friid=values(friid), group=values(group) where uid=values(uid);"
        args = [(uid, group[0], group[1]) for group in groups]
        try:
            cursor.executemany(sql, args)
        except Exception as ex:
            logging.error(str(ex))
        finally:
            cursor.close()
            self.conn.commit()
            
    def getCommTags(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        try:
            cursor.execute("select uid, group, tags from grouptags where uid='%s';" % (uid))
            res = cursor.fetchall()
            ret = {}
            for rec in res:
                ret[rec['group']] = rec['tags'].split(",")
        finally:
            cursor.close()
        return ret
    
    def updateCommTags(self, uid, groupTags):
        cursor = self.conn.cursor()
        sql = "insert into grouptags(uid, group, tags) " 
        "values(%s,%s,%s) on duplicate key update "
        "friid=values(friid), group=values(group) where uid=values(uid);"
        args = [(uid, groupTag[0], ",".join(groupTag[1])) for groupTag in groupTags]
        try:
            cursor.executemany(sql, args)
        except Exception as ex:
            logging.error(str(ex))
        finally:
            cursor.close()
            self.conn.commit()

class RedisCommInfoDao(object):
    def __init__(self, redisCluster):
        self.redisCluster = redisCluster
        
    def open(self):
        pass
    
    def getUserNeighComms(self, uid):
        redis = RedisCluster.getRedis(uid, NEIGH_COMMS_DB)
        return redis.hgetall(uid)
            
    def updateUserNeighComms(self, uid, groups):
        redis = self.redisCluster.getRedis(uid, NEIGH_COMMS_DB)
        pipe = redis.pipeline(transaction=False)
        for k, v in groups:
            pipe.hset(uid, k, v)
        pipe.execute()
            
    def getCommTags(self, uid):
        redis = RedisCluster.getRedis(uid, NEIGH_TAGS_DB)
        cache = redis.hgetall(uid)
        ret = {}
        for k, v in cache:
            ret[k] = v.split(",")
        return ret
    
    def updateCommTags(self, uid, groupTags):
        redis = self.redisCluster.getRedis(uid, NEIGH_TAGS_DB)
        pipe = redis.pipeline(transaction=False)
        for k, v in groupTags:
            pipe.hset(uid, k, v)
        pipe.execute()
