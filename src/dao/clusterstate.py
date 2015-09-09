#coding:UTF8
import MySQLdb
import time
import logging

CLUSTER_RUNNING=0
CLUSTER_SUCC=1
CLUSTER_FAIL=2
class ClusterState(object):
    def __init__(self, uid=-1, workerid=-1):
        self.uid = uid
        self.workerid = workerid
        self.state = 0
        self.time = long(time.time())
        self.errmsg = ""
    def shouldRerun(self, clusterTaskTTL, clusterGap):
        nowt = time.time()
        if ((self.state == 0 and nowt - self.time >= clusterTaskTTL) or 
                      (self.state==1 and nowt - self.time > clusterGap) or self.state == 2):
            return True
        else:
            return False
        
class ClusterStateDao(object):
    def __init__(self, conn):
        self.conn = conn
    def open(self):
        pass
    
    def setupLease(self,state, preState):
        state.time=long(time.time())
        cursor = self.conn.cursor()
        count = 0
        
        try:
            if not preState:
                count = cursor.execute("insert into clusterstate(uid, state, time, workerid) values(%s, %d, %d, %d);" % 
                               (state.uid, state.state, state.time, state.workerid))
            else:
                count = cursor.execute("update clusterstate set state = 0, time=%s, workerid=%s "\
                                       "where uid = %s and time = %s and workerid=%s" % 
                                        (state.time, state.workerid,state.uid, preState.time, preState.workerid))
        except Exception as ex:
            print str(ex)
            count = 0
        finally:
            cursor.close()
            self.conn.commit()
        
        return count > 0
                

    def extendLease(self, state):
        logging.info("extend lease")
        state.time=long(time.time())
        cursor = self.conn.cursor()
        count = 0
        try:
            count = cursor.execute("update clusterstate set state = %s, time=%s where uid = %s and workerid=%s" % 
                                        (state.state, state.time, state.uid, state.workerid))
        except Exception as ex:
            print str(ex)
        finally:
            cursor.close()
            self.conn.commit()
        if count > 0:
            logging.info("lease extended")
            return True
        else:
            logging.info("lease extended fails")
            return False
            
    def setClusterState(self, state):
        cursor = self.conn.cursor()
        try:
            sql="insert into clusterstate(uid, state, time, workerid, errmsg) values({0}, {1}, {2}, {3},'{4}') "\
                           "on duplicate key update "\
                           "state={1},time={2}, workerid={3}, errmsg='{4}';".format \
                           (state.uid, state.state, state.time, state.workerid,state.errmsg)
            logging.info("execute {0}".format(sql))
            count = cursor.execute(sql)
        #except Exception as ex:
            #print str(ex)
        finally:
            cursor.close()
            self.conn.commit()
        
    def getClusterState(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        ret = None
        try:
            cursor.execute("select * from clusterstate where uid='%s';" % (uid))
            rec = cursor.fetchone()
            
            if rec:
                ret = ClusterState(rec['uid'], rec['workerid'])
                ret.time = rec['time']
                ret.state = rec['state']
        except Exception as ex:
            print str(ex)
        
        finally:
            cursor.close()
            self.conn.commit()
        return ret
    
    def close(self):
        self.conn.close()