# encoding=utf-8
# 基于有向图的cache实现，用于缓存远端大图的一个子图
#
#
#
import csv
import sys
from cluster.graph import Graph
from lru import LRUCacheDict
class GraphCache:
    def __init__(self, dataCluster):
        self.dataCluster = dataCluster
		
        self.nodeAdj = LRUCacheDict(10240, 10)  # 邻接表 nodeID-> [nodeID]
        self.nodeProfile = LRUCacheDict(10240, 10)  # profiles
        self.edgeNum = 0

    # 插入一条边
    def addEdge(self, start, end):
        if start in self.nodeAdj:
            self.nodeAdj[start] = []
        self.nodeAdj[start].append(end)
        self.edgeNum += 1
    def addEdges(self, start, nodes):
        if start in self.nodeAdj:
            self.nodeAdj[start] = []
        self.nodeAdj[start].extend(nodes)
        
    """
    删除nodeID及其出边
    """
    def delNode(self, nodeID):
        self.edgeNum -= len(sefl.nodeAdj[nodeID])
        del self.nodeAdj[nodeID]
    
    def existNode(self, nodeID):
        return nodeID in self.nodeAdj
    
    # 获取节点集
    def nodes(self):
        return self.nodeAdj.keys()

    # 获取一个节点相邻的节点，返回(node, edgeID)
    def neighbours(self, node):
        return self.nodeAdj[node].items()

    def nodeSize(self):
        return len(self.nodeAdj)
	"""
	extract the ego-centric network of nodeID
	"""	
    def egoNetwork(self, nodeID):
        rtnGraph = Graph()
        edgeID = 0
        if self.existNode(nodeID):
            neighbours = self.loadNeighbours(nodeID)
            for neighbour in neighbours:
                rtnGraph.addEdge(edgeID, nodeID, neighbour, 1.0)
                edgeID += 1
                cNeighbours = self.loadNeighbours(neighbour)
                for cNeighbour in cNeighbours:
                    if cNeighbour in neighbours:
                        rtnGraph.addEdge(edgeID, nodeID, neighbour, 1.0)
                        edgeID += 1
    	return rtnGraph
   
    """
    get the neighbours of nodeID
    """
    def loadNeighbours(self, nodeID):
    	if not self.existNode(nodeID):
    		fetchNode(nodeID)
    	return self.neighbour(nodeID)
    
    """
    read neighbours of nodeID from redis cluster
    """
    def fetchNode(self, nodeID):
        neighbours = self.dataCluster.getRedis(nodeID).get(nodeID)
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
    g = Graph()
    g.addEdge(0 , 1 , 2, 3)
    g.addEdge(1, 2, 1, 3) 
    g.addEdge(2, 1, 3, 3)
    g.printGraph()
