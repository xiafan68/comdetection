#coding:utf8
from weibo import *
import logging
from time import sleep
from random import randint
from tokenmanager import *
from urllib2 import URLError

crawlerLogger = logging.getLogger()     

"""
"""
class WeiboCrawler(object):

    def __init__(self, tm):
        self.tm = tm
        self.curToken = None
        
    def __getattr__(self, name):
        if name != "nextToken":
            return WeiboCallWrapper(name, self)
        else:
            return self.nextToken
        
    def nextToken(self, force = False):
        if force:
            if self.curToken:
                self.tm.removeToken(self.curToken)
                self.curToken = None
        while True:
            if self.curToken and self.curToken.isValid():
                self.curToken.incre()
                return self.curToken
            else:
                if self.curToken:
                    self.tm.removeToken(self.curToken)
                self.curToken = self.tm.nextToken()

class WeiboCallWrapper(object):
    def __init__(self, name, crawler): 
        self.name = [name]
        self.crawler = crawler
    
    def __getattr__(self, name):
        self.name.append(name)
        return self
    
    def __call__(self, **kw):
        tk = self.crawler.nextToken()
        i = 0
        while i < 3 :
            i += 1
            try:
                client = APIClient(app_key=tk.uid, app_secret=tk.consumer_secret, redirect_uri=tk.redirectUrl)
                client.set_access_token(tk.access_token, tk.expires_in)
        
                cur = client
                for name in self.name:
                    cur = getattr(cur, name)
                return cur(**kw)
            except APIError, ex:
                if ex.error_code == 10006:
                    #refresh the token
                    pass
                elif ex.error_code== 20101:
                    pass
                elif ex.error_code == 10025: #wrong user
                    break
                elif ex.error_code == 20003:#wrong user
                    break
                elif ex.error_code == 10023: #rate limitx
                    tk = self.crawler.nextToken(force=True)
                elif ex.error_code == 21332:
                    tk = self.crawler.nextToken(force=True)
                elif ex.error_code == 21327:
                    tk = self.crawler.nextToken(force=True)
                # parse the error type and execute corresponding action
                crawlerLogger.error("iter %d args:%s;error:%s"%(i, str(kw),str(ex)))
                pass
            except URLError, ex :
                crawlerLogger.error(str(ex))
                i = 0
            except Exception, ex:
                crawlerLogger.error(str(ex))
                pass
            sTime = randint(1,5)
            sleep(sTime)

if __name__ == "__main__":
    c = WeiboCrawler(TokenManager())
    print str(c.statuses.user_timeline.get(uid="1707446764"))