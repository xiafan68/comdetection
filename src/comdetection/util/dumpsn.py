# encoding=utf-8
import os
import sys
from ConfigParser import ConfigParser
from dao.datalayer import DataLayer
from redisinfo import *

def dumpSN(dataDir):
    config = ConfigParser()
    cpath = os.path.join(os.getenv("COMMUNITY_HOME", os.getcwd()), "./conf/dworker.conf")
    print "load config file:", cpath
    config.read(cpath)
    dataLayer = DataLayer(config)
    snredis=dataLayer.getSNRedis()
    files= os.listdir(dataDir)
    for file in files:
        dataFile = os.path.join(dataDir, file)
        print "loading social network from file:%s"%(dataFile)
        fp = open(dataFile,"r")
        for line in fp:
            nodes = line.split("\t")
            snredis.getRedis(nodes[0], SN_DB).sadd(nodes[0],nodes[1])
        fp.close()
            
if __name__ == "__main__":
    print ",".join(sys.argv)
    #dumpSN("/Users/xiafan/快盘/dataset/user/sn")
    dumpSN(sys.argv[1])