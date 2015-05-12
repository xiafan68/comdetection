#coding:utf8

class WeiboObj(object):
    DELIMITER = "\t"
    def __init__(self, schema):
        self.fields=[""]*len(schema)
        self.schema = schema
    
    def parse(self, line):
        self.fields = line.split(Tweet.DELIMITER)
        
    def __str__(self):
        return Tweet.DELIMITER.join(self.fields)
    
    def __getattr__(self, name):
        if name in self.schema and len(self.fields) > 0:
            return self.fields[self.schema[name]]
        else:
            raise AttributeError(name)
    
    def setattr(self, name, value):
        if name in self.schema:
            if isinstance(value, basestring):
                value = value.encode('utf8')
                value = value.replace("\t", " ")
            self.fields[self.schema[name]]=str(value)

class Tweet(WeiboObj):
    def __init__(self):
        fields=["mid",'rtmid', "text", "source", "retweeted_status", "uid", "reposts_count", "comments_count", "created_at"]
        schema={}
        id=0
        for field in fields:
            schema[field] = id
            id += 1
        super(Tweet, self).__init__(schema) 
        
class UserProfile(WeiboObj):
    #for compatibility with previous format
    mapping={"location":"loc","description":"descr", "followers_count":"folCount","friends_count":"friCount","statuses_count":"statuscount","favourites_count":"favorcount", "verified_type":"vtype", "created_at":"createat"}
    def __init__(self):
        fields=["id", "name","province","city","location","description","url", "profile_image_url","gender", "followers_count", "friends_count", "statuses_count","favourites_count", "created_at", "verified","verified_type","verified_reason"]
        schema={}
        id=0
        for field in fields:
            schema[field] = id
            id += 1
        super(UserProfile, self).__init__(schema)

    def setattr(self, name, value):
        if name in UserProfile.mapping:
            name=UserProfile.mapping[name]
        super(UserProfile, self).setattr(name, value)
         