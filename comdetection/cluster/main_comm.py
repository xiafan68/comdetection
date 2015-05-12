#encoding=utf-8

import random
import struct
from graph import Graph
from community import Community
import csv
import sys

def localTest():
    f = open("../Community_latest/graph.bin","rb")
    size = struct.unpack('i', f.read(4))
    g = Graph()
    
    for i in range(0, size[0]):
       #print "degree: %d"%(struct.unpack('l', f.read(8))[0])
        i

    print "size: %d"%(size[0])
    for i in range(0, size[0]):
        start = struct.unpack('l', f.read(8))
        end = struct.unpack('l', f.read(8))
        g.addEdge(i, start[0], end[0], 1)
        #print "edge: %d -> %d"%(start[0], end[0])
    f.close()
    
    print "node size:%d"%(g.nodeSize())
    #g.printGraph()
    comm = Community(g, 0.5, 5, 5)
    comm.oneLevel()
    comm.printCommunity()

    csvReader = csv.reader(file(sys.argv[1],'rb'), csv.excel_tab)
    i = 0
    g = Graph()
    for line in csvReader:
        edgeArr = line
        g.addEdge(i, edgeArr[0], edgeArr[1], 1.0)
        i+=1
    #g.printGraph()

    comm = Community(g, 0.01, 10, 2)
    comm.initCommunity()
    comm.startCluster()
#comm.oneLevel()
    print "cluster size:%d"%(comm.clusterSize())
    comm.printCommunity()
    print "--------------------------------"
    #comm.genClusterGraph()
    
def remoteDataTest():
    
if (__name__ == "__main__"):
