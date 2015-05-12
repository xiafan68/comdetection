#encoding=utf-8

import random
from graph import Graph
#
# Cluster的modularity的值:
# A(ij)(内部边的权重之和)
# k(i)*k(j)/m2: 所有内部节点的边的权重和的平方除以m2
#
#TODO: 目前初始的community是随机决定的，如果根据用户打的label计算一个初始的community,
#或者每条边的权重等于两个节点label的jaccard距离，那么结果会不会更好?
#

class Community():
    #
    #minC:至少k个cluster
    def __init__(self, graph, min_modularity, minC, maxLevel):
        self.g = graph
        self.n2c={} #node -> community id
        #所有一个内部cluster的节点i和j的ki*kj和，就等于tot*tot
        self.tot={} #cluster中节点的所有边的权重之和
        self.inw={} # cluster内部的边之和
        self.min_modularity = min_modularity 
        self.minC = minC
        self.neigh_weight={}
        self.maxLevel = maxLevel
        
        for node in self.g.nodes():
            self.n2c[node]= node
            self.tot[node] = self.g.neighWeight(node)
            self.inw[node] = self.g.getSelfWeight(node)
            self.neigh_weight[node]=-1
        #运行状态
        self.neigh_last = 0
        self.neigh_pos={}
        

    #dnodecomm:
    #w_degree:一个节点边的的weight
    def modularity_gain(self, node, comm, dnodecomm, w_degree):
        totc=self.tot[comm]
        m2=self.g.getTotalWeight()*2
        #inw[comm] += 2*dnodecomm + g.
        return dnodecomm - totc*w_degree/(m2)

    #初始化一个community的分配
    def initCommunity(self):
        for node in self.g.nodes():
            self.n2c[node]=node

    def printCommunity(self):
        for i in self.g.nodes():
            print "node %s: cluster %s"%(i, self.n2c[i])

    #dnodecomm是什么？
    def remove(self, node, comm, dnodecomm):
        self.tot[comm] -= self.g.neighWeight(node)
        self.inw[comm] -= 2*dnodecomm + self.g.getSelfWeight(node)
        self.n2c[node] = -1

#params: 
#node:节点
#comm: 要插入的comm
#dnodecomm: 数值，只想另一个cluster的边的权重和
    def insert(self, node,  comm, dnodecomm):
        self.tot[comm] += self.g.neighWeight(node)
        self.inw[comm]  += 2*dnodecomm + self.g.getSelfWeight(node)
        self.n2c[node]=comm
        
    def modularity(self):
        q = 0.0
        m2 = self.g.getTotalWeight()*2.0
        for node in self.g.nodes():
            if (self.tot[node] > 0):
                #说明这是一个community,否则value = -1
                #为什么这里除两次m2
                q +=self.inw[node]/m2 - (self.tot[node]/m2)*(self.tot[node]/m2)
        return q

    #计算node邻近的neighbor community的权重
    def neigh_comm(self, node):
        #将运行状态初始化
        for i in range(0, self.neigh_last):
           self.neigh_weight[self.neigh_pos[i]]= -1
        self.neigh_last = 0
        
        self.neigh_pos[0]=self.n2c[node]
        self.neigh_weight[self.neigh_pos[0]]=0
        self.neigh_last = 1

        for (neighbor, edgeID) in self.g.neighbours(node):
            edge = self.g.getEdge(edgeID)
            neigh_comm=self.n2c[neighbor]
            neigh_w=edge[3]
            if (neighbor!= node):
                if (self.neigh_weight[neigh_comm]== -1):
                    self.neigh_weight[neigh_comm] = 0.0
                    self.neigh_pos[self.neigh_last] = neigh_comm
                    self.neigh_last += 1

                self.neigh_weight[neigh_comm]+=neigh_w


#执行一遍community detection算法
    def oneLevel(self):
        improvement=False
        nb_moves=0
        nb_pass_done=0
        new_mod = self.modularity()
        size = self.g.nodeSize()

        random_order=[] #干什么?
        for node in self.g.nodes():
            random_order.append(node)
        for i in range(0, size):
            #随机一个i之后的位置
            rand_pos = random.randint(0, size-i - 1) + i 
            tmp = random_order[i]
            random_order[i] = random_order[rand_pos]
            random_order[rand_pos]=tmp
            
        cont = True
        while (cont):
            cur_mod = new_mod
            nb_moves= 0
            nb_pass_done+=1

            #遍历每个节点，try to switch它的community
            for node_tmp in range(0, size):
            #for node in self.g.nodes():
                node = random_order[node_tmp] #导致node只能是整数，怎么破
                node_comm = self.n2c[node]
                w_degree = self.g.neighWeight(node)
                
                self.neigh_comm(node)
                
                self.remove(node, node_comm, self.neigh_weight[node_comm])
                
                best_comm = node_comm
                best_nblinks=0.0 #更新modularity时用到，需要增加该comm的weight
                best_increase=0.0

                for i in range(0,  self.neigh_last):
                    increase = self.modularity_gain(node, self.neigh_pos[i], self.neigh_weight[self.neigh_pos[i]], w_degree)
                    if (increase > best_increase):
                        best_comm = self.neigh_pos[i]
                        best_nblinks = self.neigh_weight[self.neigh_pos[i]]
                        best_increase = increase

                self.insert(node, best_comm, best_nblinks)

                if (best_comm != node_comm):
                    nb_moves+=1

            total_tot=0.0
            total_inw=0
            for node in self.g.nodes():
                total_tot += self.tot[node]
                total_inw += self.inw[node]
            
            new_mod = self.modularity()
            if (nb_moves > 0):
                improvement = True
            
            print "nb_moves:%d"%(nb_moves)
            print "pre:%f, cur:%f,modularity gap:%f"%(cur_mod, new_mod,new_mod - cur_mod)
            if (improvement and (new_mod - cur_mod) > self.min_modularity):
                cont = True
            else:
                cont= False
        print "#### new graph size:%d"%(self.g.nodeSize())
        print "#### cluster size:%d"%(self.clusterSize())
        print  "#### actual cluster size:%d"%(self.actualClusterSize())
        print "-------------------------------------"
        return improvement

#生成由当前graph的cluster构成的图
    def genNextCommTask(self):
        #节点:cluster;边:selfloop和节点之间的边
        newEdgeMap={}
        for node in self.g.nodes():
            for edgePair in self.g.neighbours(node):
                edge = self.g.getEdge(edgePair[1])
                oCluster= self.n2c[edgePair[0]]
                newNode=(self.n2c[node], oCluster)
                if newEdgeMap.has_key(newNode):
                    newEdgeMap[newNode]+=edge[3]
                else:
                    newEdgeMap[newNode]=edge[3]
        
        graph=Graph()
        i = 0
        #print "edgeMap"
        #print "%s"%(newEdgeMap)
        for (edge, weight) in newEdgeMap.items():
            graph.addEdge(i, edge[0], edge[1],weight)
            i+=1
        
        print graph.printGraph()

        comm = Community(graph, self.min_modularity, self.minC, self.maxLevel)
        return comm
               
    def clusterSize(self):
        clusters=set()
        for (node, cluster) in self.n2c.items():
            clusters.add(cluster)
        return len(clusters)
    
    def postProcess(self):
        outlier =self.outlierComm()
        for node in self.n2c:
            if self.n2c[node] in outlier:
                self.n2c[node]="$outlier$"

    #过小的community视为异常点
    def outlierComm(self):
        cSize={}
        for (node, cluster) in self.n2c.items():
            if cSize.has_key(cluster):
                cSize[cluster]+=1
            else:
                cSize[cluster]=1
        cSize=sorted(cSize.iteritems(), key=lambda x:x[1], reverse=False)
        
        threshold = 0.1*self.g.nodeSize()
        i = 0
        sum=0
        outlier=set()
        for (c, s) in cSize:
            sum+= s
            i+=1
            if sum < threshold:
                outlier.add(c)
        return outlier

    def actualClusterSize(self):
        cSize={}
        for (node, cluster) in self.n2c.items():
            if cSize.has_key(cluster):
                cSize[cluster]+=1
            else:
                cSize[cluster]=1
        cSize=sorted(cSize.iteritems(), key=lambda x:x[1], reverse=True)
        
        threshold = 0.8*self.g.nodeSize()
        i = 0
        sum=0
        for (c, s) in cSize:
            sum+= s
            i+=1
            if sum >= threshold:
                break
        return i+1

    def startCluster(self):
        self.oneLevel()
        self.postProcess()
        cMapChain=[]
        cMapChain.append(self.n2c)
        
        curTask = self
        i = 1
        #curTask.printCommunity()
        while curTask.clusterSize() > self.minC and i < self.maxLevel:    
            print "start level:%d"%(i)
            curTask = curTask.genNextCommTask()
            curTask.oneLevel()
            curTask.postProcess()
            #curTask.printCommunity()
            cMapChain.append(curTask.n2c)
            i+=1
        lastMap = cMapChain.pop()
        while len(cMapChain) > 0:
            topMap = lastMap
            lastMap = cMapChain.pop()
            for node in lastMap.keys():
                if topMap.has_key(lastMap[node]):
                    lastMap[node]=topMap[lastMap[node]]
            
	def getComm(self, nodeID):
		return self.n2c[nodeID]

if (__name__ == "__main__"):
    g = Graph()
    #g.addEdge(1, 0, 1, 1)
    #g.addEdge(2, 0, 2, 1)
    #g.addEdge(3, 1, 2, 1)
    #g.addEdge(4, 2,3,1)
    #g.addEdge(5, 3, 4,1)
    #g.addEdge(6,3,5,1)
    #g.addEdge(7,4,5,1)
   
    g.addEdge(1, 5, 9, 1)
    g.addEdge(2, 5, 8, 1)
    g.addEdge(3, 9, 8, 1)
    g.addEdge(4, 8, 7, 1)
    g.addEdge(5, 7, 6, 1)
    g.addEdge(6, 7, 10, 1)
    g.addEdge(7, 6, 10, 1)
    
    print "node size:%d"%(g.nodeSize())
    for i in g.nodes():
        print "node %d\n"%(i)
    comm = Community(g, 0.2, 1)
    comm.oneLevel()
    comm.printCommunity()

    
