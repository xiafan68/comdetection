#coding:utf8

import lda
import numpy as np
from scipy.sparse import coo_matrix
import jieba

"""
a exploratory class to find out the performance of word tags
"""
class GenTopic(object):
    def __init__(self):
        self.wordMap={}
        self.docList=[]
        
    """
    collect the vocabulary
    """
    def collectVocab(self, doc):
        self.data=[]
        self.row=[]
        self.col=[]
        for word in jieba.cut(doc, cut_all=False):
            if not word in self.wordMap:
                self.wordMap[word]= len(self.wordMap)
            self.col.append(self.wordMap[word])
            self.row.append(len(self.row))
            self.data.append(1)
                
    def fit(self):
        matrix = coo_matrix((self.data,(self.row, self.col)))
        self.model = lda.LDA(n_topics=20, n_iter=100, random_state=1)
        self.model.fit(matrix)  # model.fit_transform(X) is also available
        topic_word = self.model.topic_word_  # model.components_ also works

from dao.weiboobj import *        
if __name__ == "__main__":

    gen = GenTopic()
    fd= open("../../tweets.data", "r")
    utweets={}
    for line in fd.readlines():
        t = Tweet()
        t.parse(line)
        utweets[t.uid]="%s,%s"%(utweets.get(t.uid, u""), t.text)

    for doc in utweets.values():
        gen.collectVocab(doc)
    
    gen.fit()
    n_top_words=8
    topic_word=gen.model.topic_word_
    for i, topic_dist in enumerate(topic_word):
        topic_words = np.array(gen.wordMap.keys())[np.argsort(topic_dist)][:-n_top_words:-1]
        print ",".join(topic_words)
        
    doc_topic = gen.model.doc_topic_
    for i in range(10):
        print u"{} \n(top topic: {})".format(utweets.values()[i], doc_topic[i])
