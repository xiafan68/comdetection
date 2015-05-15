#coding:UTF8
import time
import MySQLdb

class UserCrawlingState(object):
    def __init__(self):
        self.lastProfileTime=time.time()- 10000
        self.lastMidsTime=time.time()- 10000
        self.lastTagTime= time.time()- 10000
        self.lastCrawlingTime = time.time()- 10000
        
class UserCrawlingStateDao(object):
    def __init__(self, conn):
        self.conn = conn
        
    """
    返回对uid的爬去状态
    """
    def getCrawlState(self, uid):
        cursor = self.conn.cursor()
        cursor.execute("select * from usercrawlingstate where uid = '%s';"%(uid))
        recs = cursor.fetchall()
        ret = UserCrawlingState()
        for rec in recs:
            ret.lastProfileTime=rec[0]
            ret.lastMidsTime = rec[1]
            ret.lastTagTime=rec[2]
        return ret