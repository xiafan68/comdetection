__author__ = 'xiafan68'
import redis

"""
simple implementation of a redis cluster, currently we don't need to use
the cluster functionality provided by redis
"""
class RedisCluster:
    def __init__(self, servers, db):
        self.servers = servers
        self.db = db

    def start(self):
        self.redisCluster=[]
        for server in self.servers:
            self.redisCluster.append(redis.StrictRedis(host=server[0], port=server[1], db=self.db))
    
        """
        get the redis client instance for key
        :return:
        """
    def getRedis(self, key, db):
        idx = self.getRedisIdx(key)
        #self.redisCluster[idx].select(db)
        return self.redisCluster[idx]

    def getRedisIdx(self, key):
        return long(key)%len(self.servers)
