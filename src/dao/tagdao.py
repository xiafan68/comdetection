# coding:UTF8
"""
用于更新tag相关的统计数据
"""

import MySQLdb
from redisinfo import TAGS_STATS_DB
class RedisTagDao(object):
    def __init__(self, redisCluster):
        self.redisCluster = redisCluster
        
    def getTagCount(self, tag):
        redis = self.redisCluster.getRedis(tag, TAGS_STATS_DB)
        return int(redis.get(tag))

    def updateTagCount(self, freq, tag):
        redis = self.redisCluster.getRedis(tag, TAGS_STATS_DB)
        redis.set(tag,freq)
    
    def close(self):
        pass
    
class DBTagDao(object):
    def __init__(self, conn):
        self.conn = conn
        
    def open(self):
        pass
    
    def getTagCount(self, tag):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute("select num from tagstats where tag='%s';" % (tag))
        res = cursor.fetchone()
        
        ret = None
        if res:
            ret = int(res['num'])
        cursor.close()
        return ret
    
    def updateTagCount(self, freq, tag):
        cursor = self.conn.cursor()
        try:
           
            sql ="insert into tagstats(tag, num) values('%s', %s) on duplicate key update num=%s;" % (tag,freq,freq)
            #print sql
            cursor.execute(sql)
        except Exception as ex:
            print str(ex)
        finally:
            cursor.close()
            self.conn.commit()
        
    def close(self):
        self.conn.close()