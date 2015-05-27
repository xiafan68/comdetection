#coding:UTF8
"""
token manager需要实现的东西：
1. token pool的管理，按照expire的最近时间返回token
2. token pool的更新，即当token没有了，或者到了需要更新token的时间时，从远程服务器上更新token
"""
from datetime import datetime
from datetime import timedelta
from util.pylangutils import singleton
import urllib2
import json
import os
import threading
from heapq import merge

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return json.JSONEncoder.default(self, obj)
        
"""
负责刷新token的类
"""
class Token(object):
    """
     String consumer_key;
    public String consumer_secret;
    public String token;
    public String secret;
    public String redirectUrl;
    int count;number of invocations
    long timestamp;
    """
    def __init__(self, tokenDict):
        self.__dict__.update(tokenDict)
        self.create_time = datetime.strptime(self.create_time, "%Y-%m-%d %H:%M:%S")
        self.expire_time = self.create_time + timedelta(seconds=long(self.expires_in))
        if 'count' not in tokenDict:
            self.count = 0 
            
    def incre(self):
        self.count += 1
            
    """
    返回当前token是否仍然有效
    """    
    def isValid(self):
        now = datetime.now()
        if self.count < 5000 and self.expire_time > now:
            return True
        else:
            return False
         
    def __cmp__(self, other):
        ret = cmp(self.expire_time, other.expire_time)
        if ret == 0:
            ret = cmp(self.uid, other.uid)
        return ret

@singleton
class TokenManager(object):
    def __init__(self, config):
        self.tokencache = []
        self.urlseeds = config.get('crawl','urlseeds')
        self.tokenDir = config.get('crawl', 'tokendir')
        self.tokenLock = threading.Lock()
        self.pos = 0
        self.loadTokens()
        
    def saveTokens(self):
        with open(os.path.join(self.tokenDir, "tokens.txt"), "w") as fd:
            for token in self.tokencache:
                fd.write(json.dumps(token.__dict__,encoding="UTF-8", ensure_ascii=False, cls = ComplexEncoder))
                fd.write("\n")
                        
    def loadTokens(self):
        tokenFile = os.path.join(self.tokenDir, "tokens.txt")
        if not os.path.exists(tokenFile):
            return
        with open(tokenFile) as fd:
            for line in fd.readlines():
                self.tokencache.append(Token(json.loads(line)))
                
    def pullRemoteTokensOfUrl(self, url):
        req = urllib2.Request(url)
        res = urllib2.urlopen(req)
        content =res.read()      
        reply = json.loads(content)
        if reply['error'] == 0:
            newTokens = []
            for tokenDict in  reply['data']:
                token = Token(tokenDict)
                token.consumer_secret='1695e3222429c090a86a4ad5a10f01e7'
                token.secret='1695e3222429c090a86a4ad5a10f01e7'
                token.redirectUrl='http://mblog.city.sina.com.cn/index.php?app=admin&mod=Account&act=callback'
                newTokens.append(token)
            newTokens = sorted(filter(lambda token:token.isValid(), newTokens))
            self.tokencache = list(merge(self.tokencache, newTokens))
            self.saveTokens()
    
    def pullRemoteTokens(self):
        self.pullRemoteTokensOfUrl(self.urlseeds)
    
    def removeToken(self, token):
        self.tokencache.remove(token)
        
    def nextToken(self):
        with self.tokenLock:
            while True:
                tryCount= 0
                while tryCount < len(self.tokencache):
                    self.pos = ++self.pos % len(self.tokencache)
                    token =  self.tokencache[self.pos]
                    if token.isValid():
                        return token
                    else:
                        tryCount += 1
                self.tokencache = filter(lambda token:token.isValid(), self.tokencache)
                self.pullRemoteTokens()

if __name__ == "__main__":
    from ConfigParser import ConfigParser
    config = ConfigParser()
    print os.getcwd()
    cpath = os.path.join(os.getcwd(), "../../../conf/dworker.conf")
    print "load config file:", cpath
    config.read(cpath)
    tm = TokenManager(config)
    print str(tm.nextToken())