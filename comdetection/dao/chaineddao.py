#coding:UTF8

"""
实现一个chained dao类，
1. 在访问数据时，按照chain顺序地取数据，直到在某一层都到数据
2. 随后翻转更新每一层的数据
"""

class ChainedCall(object):
    def __init__(self, daos, method):
        self.daos
class ChainedDao(object):
    def __init__(self, daos):
        self.daos = daos
        
    def __getattr__(self, method):
        self.method = method
        return self
        
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