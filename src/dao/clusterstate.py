#coding:UTF8
import MySQLdb

class ClusterStateDao(object):
    def __init__(self, conn):
        self.conn = conn
    def open(self):
        pass
    
    def setClusterState(self, uid, state, time):
        cursor = self.conn.cursor()
        cursor.execute("insert into clusterstate(uid, state, time) values(%s, %d, %d) "
                       "on duplicate key update "
                       "state=values(state),time=values(time) where uid= values(uid);" % 
                       (uid, state, time))
        cursor.close()
    
    def getClusterState(self, uid):
        cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        try:
            cursor.execute("select uid, state, time from clusterstate where uid='%s';" % (uid))
            res = cursor.fetchall()
            
            ret = None
            for rec in res:
                ret=(rec['state'], rec['time'])
                break
        except Exception as ex:
            print str(ex)
        
        finally:
            cursor.close()
        return ret
    
    def close(self):
        self.conn.close()