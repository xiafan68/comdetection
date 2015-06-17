# coding:utf8

class WeiboObj(object):
    DELIMITER = "\t"
    def __init__(self, schema):
        self.fields = [""] * len(schema)
        self.schema = schema
    
    def parse(self, line):
        line = line.decode('utf8')
        self.fields = line.split(Tweet.DELIMITER)
        
    def __str__(self):
        ret = []
        for field in self.fields:
            if isinstance(field, unicode):
                field = field.decode('utf8')
            ret.append(str(field))
        return Tweet.DELIMITER.join(ret)
    
    def __getattr__(self, name):
        if name in self.schema and len(self.fields) > 0:
            return self.fields[self.schema[name]]
        else:
            raise AttributeError(name)
    
    def getSQLColums(self):
        return ",".join(self.schema)
    def getSQLValues(self):
        vals = []
        for idx in self.schema.values():
            if isinstance(self.fields[idx],unicode):
                vals.append("'%s'" %(self.fields[idx].encode('utf8', 'ignores')))
            elif isinstance(self.fields[idx], basestring):
                vals.append("'%s'" %(self.fields[idx]))
            else:
                vals.append(str(self.fields[idx]))
        return ",".join(vals)
    
    def getUpdate(self, keyField):
        updates = []
        for (field, idx) in self.schema.items():
            if field != keyField:
                v = self.fields[idx]
                strfmt = "%s=%s"
                if isinstance(v, basestring):
                    strfmt = "%s='%s'"
                if isinstance(v,unicode):
                    v = v.encode('utf8', 'ignores')
                updates.append(strfmt % (field, str(v)))
        return ",".join(updates)
                
    def setattr(self, name, value):
        if name in self.schema:
            if not isinstance(value, unicode) and isinstance(value, basestring):
                try:
                    value = value.decode('utf8', 'ignore')
                except UnicodeEncodeError, ex:
                    print value
            if isinstance(value, unicode):
                value = value.replace("\t", " ")
            self.fields[self.schema[name]] = value

class Tweet(WeiboObj):
    def __init__(self):
        fields = ["mid", 'rtmid', "text", "source", "uid", "reposts_count", "comments_count", "created_at"]
        schema = {}
        id = 0
        for field in fields:
            schema[field] = id
            id += 1
        super(Tweet, self).__init__(schema) 

from datetime import datetime
import time
class UserProfile(WeiboObj):
    # for compatibility with previous format
    mapping = {"location":"location", "description":"descr", "followers_count":"folCount", 
               "friends_count":"friCount", "statuses_count":"statusCount", 
               "favourites_count":"favorCount", "verified_type":"vtype", "created_at":"createat","profile_image_url":'pimg_url'}
    def __init__(self):
        fields = ["uid", "name", "province", "city", "location", "descr", 
                  "url", "pimg_url", "gender", "folCount", "friCount",
                "statusCount", "favorCount", "createat", "verified", 
                "vtype", "verified_reason", "lastcrawltime", "crawlstate"]
        schema = {}
        idx = 0
        for field in fields:
            schema[field] = idx
            idx += 1
        super(UserProfile, self).__init__(schema)
        super(UserProfile, self).setattr("lastcrawltime",-100000)

    def setattr(self, name, value):
        if name in UserProfile.mapping:
            name = UserProfile.mapping[name]
        if name == 'createat':
            try:
                value = long(time.mktime(datetime.strptime(value, '%a %b %d %H:%M:%S +0800 %Y').timetuple()))*1000
            except Exception, ex:
                pass
        super(UserProfile, self).setattr(name, value)

if __name__ == "__main__":
    u = UserProfile()
    for field in UserProfile.mapping.values():
        u.setattr(field, field)
    u.setattr("id", 1)
    print u.getUpdate("name")
    strtime = 'Tue Oct 12 23:07:12 +0800 2010'
    from datetime import datetime
    print time.mktime(datetime.strptime(strtime, '%a %b %d %H:%M:%S +0800 %Y').timetuple())
