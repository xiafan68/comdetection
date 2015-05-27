__author__ = 'xiafan68'
import redis

"""
simple implementation of a redis cluster, currently we don't need to use
the cluster functionality provided by redis
>>>config = ConfigParser()
>>>cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
>>>config.read(cpath)
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
        if not (idx, db) in self.clientmap:
            server = self.servers[idx]
            self.clientmap[(idx,db)]=redis.StrictRedis(host=server[0], port=server[1], db=db)
        return self.clientmap[(idx, db)]
    
    def getRedisIdx(self, key):
        return long(key)%len(self.servers)

    def close(self):
        for client in self.clientmap.values():
            client.close()
if __name__ == "__main__":
    from ConfigParser import ConfigParser
    import os
    import threading
    import logging
    
    cslogger=logging.getLogger("cluster server")    
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
    