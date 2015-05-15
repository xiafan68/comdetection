__author__ = 'xiafan68'
import redis

"""
simple implementation of a redis cluster, currently we don't need to use
the cluster functionality provided by redis
"""
class RedisCluster:
    def __init__(self, servers):
        self.servers = servers
        self.clientmap={}

    def start(self):
        pass
    
        """
        get the redis client instance for key
        :return:
        """
    def getRedis(self, key, db):
        idx = self.getRedisIdx(key)
        if not (key, db) in self.clientmap:
            server = self.servers[idx]
            self.clientmap[(key,db)]=redis.StrictRedis(host=server[0], port=server[1], db=db)
        return self.clientmap[(key, db)]
    
    def getRedisIdx(self, key):
        return long(key)%len(self.servers)

    def close(self):
        for client in self.clientmap.values():
            client.close()