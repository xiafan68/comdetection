# encoding=utf-8
# 基于有向图的cache实现，用于缓存远端大图的一个子图
#
#
#
from cluster.graph import Graph
from xredis.RedisCluster import RedisCluster
from lru import LRUCacheDict
import logging
from util.strutil import *
from redisinfo import *
from dao.datalayer import *
 
logger = logging.getLogger(__name__)

class GraphCache:
    def __init__(self, dataLayer):
        self.dataCluster = dataLayer.getSNRedis()
        self.userDao = dataLayer.getCachedCrawlUserDao()

        self.nodeAdj = LRUCacheDict(102400, 10)  # 邻接表 nodeID-> [nodeID]
        self.nodeProfile = LRUCacheDict(102400, 10)  # profiles
        self.edgeNum = 0

    # 插入一条边
    def addEdge(self, start, end):
        if not self.nodeAdj.has_key(start):
            self.nodeAdj[start] = set()
        self.nodeAdj[start].add(end)
        self.edgeNum += 1
    
    def addEdges(self, start, nodes):
        if not self.nodeAdj.has_key(start):
            self.nodeAdj[start] = set()
        self.nodeAdj[start].update(nodes)
        self.edgeNum += len(nodes)
        
    """
    删除nodeID及其出边
    """
    def delNode(self, nodeID):
        self.edgeNum -= len(self.nodeAdj[nodeID])
        del self.nodeAdj[nodeID]
    
    def existNode(self, nodeID):
        if not self.nodeAdj.has_key(nodeID):
            self.fetchNode(nodeID)
        return self.nodeAdj.has_key(nodeID)
    
    # 获取节点集
    def nodes(self):
        return self.nodeAdj.keys()

    # 获取一个节点相邻的节点，返回(node, edgeID)
    def neighbours(self, node):
        return self.nodeAdj[node]

    def nodeSize(self):
        return len(self.nodeAdj)
    """
    extract the ego-centric network of nodeID
    """    
    def egoNetwork(self, nodeID):
        nodeID = strToUnicode(nodeID)
        rtnGraph = Graph()
        edgeID = 0
        if self.existNode(nodeID):
            neighbours = self.loadNeighbours(nodeID)
            for neighbour in neighbours:
                neighbour=strToUnicode(neighbour)
                rtnGraph.addEdge(edgeID, nodeID, neighbour, 1.0)
                edgeID += 1
                cNeighbours = self.loadNeighbours(neighbour)
                for cNeighbour in cNeighbours:
                    if cNeighbour in neighbours:
                        rtnGraph.addEdge(edgeID, neighbour, cNeighbour, 1.0)
                        edgeID += 1
        
        return rtnGraph

    def loadNodesName(self, nodes):
        profiles={}
        logger.info("searching for nodes:%s"%str(nodes))
        for node in nodes:
            rec = self.userDao.getUserProfile(node)
            profiles[node]=rec['name']
        return profiles

    def loadProfiles(self, graph):
        profiles={}
        for node in graph.nodes():
            rec = self.userDao.getUserProfile(node)
            profiles[node]=rec
        return profiles

    """
    get the neighbours of nodeID
    """
    def loadNeighbours(self, nodeID):
        if not self.existNode(nodeID):
            self.fetchNode(nodeID)
            
        if not self.existNode(nodeID):
            return set()
        else:
            return self.neighbours(nodeID)
        
    """
    read neighbours of nodeID from redis cluster
    """
    def fetchNode(self, nodeID):
        redis= self.dataCluster.getRedis(nodeID, SN_DB)
        neighbours = redis.smembers(nodeID)
        #self.dataCluster.returnRedis(nodeID, redis)
        if len(neighbours) > 0:
            self.addEdges(nodeID, neighbours)
        
if __name__ == "__main__":
    # csvReader = csv.reader(file(sys.argv[1],'rb'), csv.excel_tab)
    # i = 0
    # g = Graph()
    # for line in csvReader:
    #    edgeArr = line
    #    g.addEdge(i, edgeArr[0], edgeArr[1], 1.0)
    #    i+=1
    # g.printGraph()
    dataCluster = RedisCluster([ ("10.11.1.51", 6379),
            ("10.11.1.52", 6379), ("10.11.1.53", 6379), ("10.11.1.54", 6379), ("10.11.1.55", 6379),
           ("10.11.1.56", 6379), ("10.11.1.57", 6379), ("10.11.1.58", 6379), ("10.11.1.61", 6379),
            ("10.11.1.62", 6379), ("10.11.1.63", 6379)], 0)
    dataCluster.start()
    graphCache = GraphCache(dataCluster)
    print str(graphCache.egoNetwork("1000048833"))
