# encoding=utf-8
# simple undirected graph implemenataion with weighted edge
#
#
#

class Graph():
    def __init__(self):
        self.nodeAdj = {}  # 邻接表 nodeID-> [edgeID]
        self.nodeWeight = {}  # node出去loop边的权重之和，空间换时间
        self.nodeSelfWeight = {}  # node的loop边的权重
        self.edges = {}
        self.totalWeight = float(0.0)
        
    # 插入一条边
    def addEdge(self, id, start, end, weight):
        if self.existEdge(start, end):
            return
        self.totalWeight += float(weight)
        if start == end:
            self.nodeSelfWeight[start] = weight
            self.insertNode(start)
        else:
            self.edges[id] = (id, start, end, float(weight))      
            self.installNodeEdge(start, id, end, weight)
            self.installNodeEdge(end, id, start, weight)
      
    def udpateEdgeWeight(self, edge, newWeight):
        self.totalWeight= newWeight - edge[3]
        if edge[1] == edge[2]:
            self.nodeSelfWeight[edge[1]] = newWeight
        else:
            self.nodeWeight[edge[1]]+=newWeight-edge[3] 
            self.nodeWeight[edge[2]]+=newWeight-edge[3]
             
    def existEdge(self, start, end):
        if self.nodeAdj.has_key(start) and self.nodeAdj[start].has_key(end):
            return True
        return False

    def insertNode(self, node):
        if self.nodeAdj.has_key(node) == False:
            self.nodeAdj[node] = {}
            self.nodeWeight[node] = 0.0
    def installNodeEdge(self, node, edgeID, end, weight):
        self.insertNode(node) 
        if self.nodeAdj[node].has_key(end) == False:
            self.nodeAdj[node][end] = edgeID
            self.nodeWeight[node] += float(weight)

        
    def getSelfWeight(self, node) :
        if (self.nodeSelfWeight.has_key(node)):
            return self.nodeSelfWeight[node]
        else:
            return 0.0

    # 每条边只算了一次权重，和另一个实现
    def getTotalWeight(self):
        return self.totalWeight
    
    # 获取邻居节点集
    def nodes(self):
        return self.nodeAdj.keys()

    # 获取一个节点相邻的节点，返回(node, edgeID)
    def neighbours(self, node):
        return self.nodeAdj[node].items()

    # 所有邻接边(除去selfloop边)的权重之和
    def neighWeight(self, node):
        return self.nodeWeight[node]

    def getEdge(self, nodeID):
        return self.edges[nodeID]
    
    def edges(self):
        return self.edges
    
    def nodeSize(self):
        return len(self.nodeAdj)
    def edgeSize(self):
        return len(self.edges)

    def __str__(self):
        ret = "graph:*****************\n"
        for node in self.nodes():
            ret += str.format("%s -> " % (node))
            for edgePair in self.neighbours(node):
                edge = self.getEdge(edgePair[1])
                ret += str.format("(%s|%f)," % (edgePair[0], edge[3]))
            if self.getSelfWeight(node) > 0.0:
                ret += str.format("(%s|%f)," % (str(node), self.getSelfWeight(node)))
            ret +="\n----------------\n"
        ret += "***********************\n"
        return ret
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
