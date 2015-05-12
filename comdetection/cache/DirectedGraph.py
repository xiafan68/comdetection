#encoding=utf-8
#基于有向图的cache实现，用于缓存远端大图的一个子图
#
#
#
import csv
import sys

class DirectedGraph():
    def __init__(self):
        self.nodeAdj={} #邻接表 nodeID-> [nodeID]
        self.nodeProfile={}#profiles
        self.edgeNum = 0
    #插入一条边
    def addEdge(self, start, end):
        if start in self.nodeAdj:
            self.nodeAdj[start]=[]
        self.nodeAdj[start].append(end)
        self.edgeNum += 1
    
    """
    删除nodeID及其出边
    """
    def delNode(self, nodeID):
    	self.edgeNum -= len(sefl.nodeAdj[nodeID])
        del self.nodeAdj[nodeID]
    
    def existNode(self, nodeID):
        return nodeID in self.nodeAdj
    
    #获取节点集
    def nodes(self):
        return self.nodeAdj.keys()

    #获取一个节点相邻的节点，返回(node, edgeID)
    def neighbours(self, node):
        return self.nodeAdj[node].items()

    def nodeSize(self):
        return len(self.nodeAdj)
        