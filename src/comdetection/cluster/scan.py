#encoding=utf-8
import random
import Queue
from graph import Graph
from compiler.ast import Node
import math
"""
implementation of the scan algorithm
"""
class scan(object):
    def __init__(self, g):
        self.g = g
        self.n2c=None
        
    def computeCommunity(self):
        maxModularity = -100000
        for e in range(1, 10, 1):
            e = e/10.0
            neighbourMap = self.scanOneIter(2, e)
            curMod = self.modularity(neighbourMap)
            if curMod > maxModularity:
                maxModularity = curMod
                self.postProcess(neighbourMap)
                self.n2c = neighbourMap
    
    def postProcess(self, neighbourMap):
        for node in neighbourMap.keys():
            if neighbourMap[node] < 0:
                neighbourMap[node] = -1 #mark as outlier
        
    def modularity(self, mapping):
        clusters = {}
        for (k, v) in mapping.items():
            if not (v in clusters):
                clusters[v] = []
            clusters[v].append(k)
        
        ret = 0.0
        for (com, nodes) in clusters.items():
            #if com >= 0:
            inClusterEdges = 0.0
            clusterDegree = 0.0
            for node in nodes:
                nSet = self.g.neighbourNodes(node)
                clusterDegree += len(nSet)
                for neighb in nSet:
                    if mapping[neighb] == mapping[node]:
                        inClusterEdges += 1
            if len(nodes) > 1:
                inClusterEdges = inClusterEdges / 2
            ret += inClusterEdges / self.g.getEdgeNum() - math.pow(2, clusterDegree / (2 * self.g.getEdgeNum()))
        return ret
    
    """
    u: the threshold for size of neighbour
    e: the threshold for the similarity of edge
    """
    def scanOneIter(self, u, e):
        state = {}

        outlierID = -1
        queue = Queue.Queue() 
        for node in self.g.nodes():
            if node in state:
                continue
            elif self.isCore(node, u, e):
                state[node] = node
                queue.put(node)
                while not queue.empty():
                    curNode = queue.get()
                    for neighNode in self.neighbour(curNode, e):
                        if state.get(neighNode, -2) < 0:
                            state[neighNode] = node
                            if state.get(neighNode,-2) == -1:
                                queue.put(neighNode)
            else:
                state[node] = outlierID
                outlierID -= 1
        return state
        
    def isCore(self, node, u, e):
        if len(self.neighbour(node, e)) > u:
            return True
        return False
    
    def neighbour(self, node, e):
        ret = []
        ineigh = set(self.g.neighbourNodes(node))
        ineigh.add(node)
        for nnode in ineigh:
            oneigh = set(self.g.neighbourNodes(nnode))
            oneigh.add(nnode)
            sim = len(oneigh & ineigh) / float(len(oneigh | ineigh))
            if sim > e:
                ret.append(nnode)
        return ret
            
