graph接口：
* 节点为integer,边由边的id，start, end,weight构成
* 提供边的插入，删除操作
* 针对节点：
  * 遍历一个节点的neighbour

* 根据start, end，获取边的权重


计算过程：

1. 计算一个node到所有neigh_comm的权重，选择一个community，试试将其加入是否能够
提升modularity.
