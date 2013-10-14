#encoding=utf-8
#simple undirected graph implemenataion with weighted edge
#
#
#
class Graph():
    def __init__(self):
        self.nodeAdj={} #邻接表 nodeID-> [edgeID]
        self.nodeWeight={}#node出去loop边的权重之和，空间换时间
        self.nodeSelfWeight={}#node的loop边的权重
        self.edges={}
        self.totalWeight = float(0.0)
        
    #插入一条边
    def addEdge(self,edgeID, start, end, weight):
        if self.existEdge(start, end):
            return
        
        self.totalWeight +=  float(weight)
        self.edges[edgeID]=(edgeID, start, end, float(weight))
      
        self.insertNode(start, edgeID ,2, weight)
        self.insertNode(end, edgeID, 1, weight)
      
        if start == end:
            self.nodeSelfWeight[start]=weight
   
    def existEdge(self, start, end):
        if self.nodeAdj.has_key(start) and self.nodeAdj[start].has_key(end):
            return True
        return False

    def insertNode(self, nodeS, edgeID,oIdx, weight):
        if self.nodeAdj.has_key(nodeS) == False:
            self.nodeAdj[nodeS] = {}
            self.nodeWeight[nodeS]=0.0
        if self.nodeAdj[nodeS].has_key(edgeID)==False:
            self.nodeAdj[nodeS][edgeID] = oIdx
            self.nodeWeight[nodeS]+=float(weight)

    def getSelfWeight(self, node) :
        if (self.nodeSelfWeight.has_key(node)):
            return self.nodeSelfWeight[node]
        else:
            return 0.0

    #每条边只算了一次权重，和另一个实现
    def getTotalWeight(self):
        return self.totalWeight
    
    #获取邻居节点集
    def nodes(self):
        return self.nodeAdj.keys()

    #获取一个节点相邻的边
    def neighbours(self, node):
        return self.nodeAdj[node].items()

    #所有邻接边(除去selfloop边)的权重之和
    def neighWeight(self, node):
        return self.nodeWeight[node]

    def getEdge(self, id):
        return self.edges[id]
    def nodeSize(self):
        return len(self.nodeAdj)
    def edgeSize(self):
        return len(self.edges)


if __name__ == "__main__":
    g =Graph()
    g.addEdge(0, 0, 1,1)
    g.addEdge(0, 0, 1,1)
    #保证不能重复加入同一条边
    print "node %s:weight:%f"%(0, g.neighWeight(0))
    g.addEdge(0, 0, 2,1)
    print "node %s:weight:%f"%(0, g.neighWeight(0))
    g.addEdge(0, 0, 3,1)
    print "node %s:weight:%f"%(0, g.neighWeight(0))
    g.addEdge(0, 1, 2,1)
    g.addEdge(0, 1, 3,1)
    g.addEdge(0, 1, 4,1)
    print "node %s:weight:%f"%(1, g.neighWeight(1))
