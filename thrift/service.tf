namespace py network

struct Worker{
1:string ipAddr,
2: i16 port
}

//记录每个worker的工作状态，包括画了多少时间处理了多少对象，以及最近处理的对象数
struct WorkStatus{
1: Worker worker,
  2: i32 totalTime,
  3: i32 totalIds,
  4: i32 recentTime,
  5: i32 recentIds,
  6:i16 jobQueueID
}

service ClusterClient{
//report the work status of this work node
WorkStatus reportStatus(),

void reAssignJob(1:i16 jobQueueID),
//compute the cluster for nodeID
void clusterForNode(1:i32 nodeID),
//stop processing
void stop(),
}
