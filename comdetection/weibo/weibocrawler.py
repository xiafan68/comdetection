#coding:utf8
from weibo import *
import logging
from time import sleep
from random import randint

crawlerLogger = logging.getLogger("weibocrawler")
def weiboCrawler():
    pass

class Token(object):
    """
     String consumer_key;
    public String consumer_secret;
    public String token;
    public String secret;
    public String redirectUrl;
    int count;
    long timestamp;
    """
    def __init__(self, **kw):
        for k, v in kw.iteritems():
            setattr(self, k, v)
            
"""
manage token refresh
"""  
class TokenManager(object):
    
    def nextToken(self):
        return Token(consumer_key="", consumer_secret='', 
                     token="2.",
                     secret='',
                     redirectUrl='',
                     count=1, timestamp=1)
    
"""
"""
class WeiboCrawler(object):

    def __init__(self, tm):
        self.tm = tm
        
    def __getattr__(self, name):
        return WeiboCallWrapper(name, self.tm)


class WeiboCallWrapper(object):
    def __init__(self, name, tm): 
        self.name = [name]
        self.tm = tm
    
    def __getattr__(self, name):
        self.name.append(name)
        return self
    
    def __call__(self, **kw):
        tk = self.tm.nextToken()
        for i in range(3) :
            try:
                client = APIClient(app_key=tk.consumer_key, app_secret=tk.consumer_secret, redirect_uri=tk.redirectUrl)
                client.set_access_token(tk.token, 2619960)
        
                cur = client
                for name in self.name:
                    cur = getattr(cur, name)
                return cur(**kw)
            except APIError, ex:
                if ex.error_code == 10006:
                    #refresh the token
                    pass
                if ex.error_code== 20101:
                    pass
                # parse the error type and execute corresponding action
                crawlerLogger.error("iter %d args:%s;error:%s"%(i, str(kw),str(ex)))
                pass
            except Exception, ex:
                crawlerLogger.error(str(ex))
                pass
            sTime = randint(1,9)
            #sleep(sTime)

if __name__ == "__main__":
    c = WeiboCrawler(TokenManager())
    print str(c.statuses.user_timeline.get(uid="1707446764"))