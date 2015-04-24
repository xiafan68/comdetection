#encoding=utf-8
#基于有向图的cache实现，用于缓存远端大图的一个子图
#
#
#
import csv
import sys

class GraphCache(DirectedGraph):
    def __init__(self):
        self.nodeAdj={} #邻接表 nodeID-> [nodeID]
        self.nodeProfile={}#profiles
        self.edgeNum = 0

    def egoNetwork(self, nodeID):
        
if __name__ == "__main__":
    #csvReader = csv.reader(file(sys.argv[1],'rb'), csv.excel_tab)
    #i = 0
    #g = Graph()
    #for line in csvReader:
    #    edgeArr = line
    #    g.addEdge(i, edgeArr[0], edgeArr[1], 1.0)
    #    i+=1
    #g.printGraph()
    g = Graph()
    g.addEdge(0 ,1 ,2, 3)
    g.addEdge(1, 2, 1, 3) 
    g.addEdge(2, 1, 3, 3)
    g.printGraph()
