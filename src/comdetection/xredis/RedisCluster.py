__author__ = 'xiafan68'
import redis
import math
"""
simple implementation of a redis cluster, currently we don't need to use
the cluster functionality provided by redis
"""
class RedisCluster:
    def __init__(self, servers):
        self.servers = servers
        self.clientmap={}
        self.poolMap={}
    def start(self):
        pass
    
        """
        get the redis client instance for key
        :return:
        """
    def getRedis(self, key, db):
        idx = self.getRedisIdx(key)
        if not (idx, db) in self.clientmap:
            server = self.servers[idx]
            self.clientmap[(idx,db)]=redis.StrictRedis(connection_pool=self.getPool((idx,db)),
                                                       host=server[0], port=server[1], db=db)
        return self.clientmap[(idx, db)]
    
    def getPool(self, key):
        if not key in self.poolMap:
            server = self.servers[key[0]]
            self.poolMap[key]=redis.ConnectionPool(host=server[0], port=server[1],db=key[1])
        return self.poolMap[key]

    def getRedisIdx(self, key):
        try:
            key = long(key)
        except:
            key = abs(hash(key))
        return key%len(self.servers)

    def close(self):
        for client in self.clientmap.values():
            client.close()
            
if __name__ == "__main__":
    from ConfigParser import ConfigParser
    import os
    import threading
    import logging
    
    cslogger=logging.getLogger()    
    cslogger.setLevel(logging.INFO)
    hdr = logging.StreamHandler()
    hdr.setFormatter(logging.Formatter("[%(asctime)s] %(name)s:%(levelname)s : %(message)s"))
    cslogger.addHandler(hdr)
    config = ConfigParser()
    cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
    config.read(cpath)
    snservers=[server.strip() for server in config.get('sncache','hosts').split(",")]
    snports=[int(port) for port in config.get('sncache', 'ports').split(',')]
    cluster = RedisCluster(zip(snservers, snports))
    assert cluster.getRedis(0, 1)== cluster.getRedis(0, 1)
    def multiThreadGet():
        for i in range(10000):
            cslogger.info("%s invoke %d"%(threading.currentThread().name, i))
            cluster.getRedis(0, 1)
            
    for i in range(4):
        t = threading.Thread(name = str(i),target=multiThreadGet())
        t.setDaemon(True)
        t.start()
    