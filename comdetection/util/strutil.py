#coding:UTF8

def strToUnicode(val):
    if not(isinstance(val, unicode)) and isinstance(val, basestring):
        return val.decode('utf8')
    return val