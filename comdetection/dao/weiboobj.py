
class WeiboObj(object):
    DELIMITER = "\t"
    def __init__(self, schema):
        self.fields=[]
        self.schema = schema
    
    def parse(self, line):
        self.fields = line.split(Tweet.DELIMITER)
        
    def __str__(self):
        return Tweet.DELIMITER.join(self.fields)
    
    def __getattr__(self, name):
        if name in Tweet.schema and len(self.fields) > 0:
            return fields[Tweet.schema[name]]
        else:
            raise AttributeError(name)
    
    def setattr(self, name, value):
        self.fields[self.schema[name]]=value

class Tweet(WeiboObj):
    def __init__(self):
        super.__init__(self, {"mid":0, "time":1, "location":2, "uid":3,"content":4}) 
        
class User(WeiboObj):
    def __init__(self):
        super.__init__(self, {"uid":0, "name":1, "location":2, "descr":3,}) 