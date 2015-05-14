#coding:UTF8

"""
实现一个chained dao类，
1. 在访问数据时，按照chain顺序地取数据，直到在某一层都到数据
2. 随后翻转更新每一层的数据
"""

class DaoCallWrapper(object):
    def __init__(self, method, daos):
        self.method = method
        self.daos = daos
    def __call__(self, **kw):
        ret = None
        calledDaos = []
        for dao in self.daos:
            calledDaos.append(dao)
            attr = getattr(dao, self.method)
            ret = attr(**kw)
            if ret:
                break
        if ret:
            calledDaos.pop()
            method = self.method.replace("get","update")
            while calledDaos:
                dao = calledDaos.pop()
                try:
                    attr = getattr(dao, method)
                    attr(ret)
                except Exception,ex:
                    
                    pass
        return ret    

class ChainedDao(object):
    def __init__(self, daos):
        self.daos = daos
        
    def __getattr__(self, method):
        return DaoCallWrapper(method, self.daos)
        
    

class test1:
    def getTest(self):
        return None
    def updateTest(self, data):
        print "test1 update %s"%(data)
        
class test2:
    def getTest(self):
        return "test2"
    def updateTest(self):
        pass 
if __name__ == "__main__":
    call = ChainedDao([test1(),test2()])
    print call.getTest()
    
    