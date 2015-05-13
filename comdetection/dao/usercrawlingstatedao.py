#coding:UTF8
import time
import MySQLdb

class UserCrawlingState(object):
    def __init__(self):
        self.lastProfileTime=time.time()
        self.lastMidsTime=time.time()
        self.lastTagTime= time.time()
        self.lastCrawlingTime = time.time()
        
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
        for rec in recs:
            ret = UserCrawlingState()
            ret.lastProfileTime=rec[0]
            ret.lastMidsTime = rec[1]
            ret.lastTagTime=rec[2]
            