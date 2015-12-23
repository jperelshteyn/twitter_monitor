from __future__ import division
import sys
from math import log
from textblob import TextBlob as tb
from nltk.corpus import stopwords
from unicodedata import normalize
from nltk import stem
from nltk import wordnet
from numpy import median
from pymongo import MongoClient
from bson.objectid import ObjectId
from time import time
from dateutil.parser import parse
from datetime import timedelta, datetime

lemma = wordnet.WordNetLemmatizer()

stop_words = set(stopwords.words('english'))
stop_chars = {',', '.', '?', '!', ';', ':', "'", '"'}
previous_headlines = None
blob_list = []


def tf(word, blob):
    return blob.words.count(word) / len(blob.words)


def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob)


def idf(word, bloblist):
    return log(len(bloblist) / (1 + n_containing(word, bloblist)))


def tfidf(word, blob, bloblist):
    return tf(word, blob) * idf(word, bloblist)


def clean_word(word):
    clean_word = ''
    for c in u_to_a(word).lower():
        if c not in stop_chars:
            clean_word += c
    return clean_word
        

def clean_headline(headline):
    clean_headline = ''
    for w in headline.split():
        if w not in stop_words:
            w = clean_word(w)
            clean_headline += lemma.lemmatize(w.lower()) + ' '
    return clean_headline[:-1]


def split_headline(headline):
    return clean_headline(headline).split()


def blob_headline(headline):
    return tb(clean_headline(headline))


def get_previous_headlines(with_ids=False):
    client = MongoClient()
    db = client.twitter_news
    news_coll = db.news
    headlines = []
    for item in news_coll.find():
        if with_ids:
            headlines.append((item['_id'], clean_headline(item['headline'])))
        else:
            headlines.append(clean_headline(item['headline']))
    return headlines


def score_headline(headline_blob):
    global previous_headlines
    global blob_list
    if not previous_headlines:
        previous_headlines = get_previous_headlines()
    if not blob_list:
        for p_headline in previous_headlines:
            blob = tb(p_headline)
            blob_list.append(blob) 
    tfidf_scores = []
    for w in headline_blob.words:
        tfidf_scores.append((w, tfidf(w, headline_blob, blob_list)))
    tfidf_scores.sort(key=lambda tup: tup[1], reverse=True)
    return tfidf_scores


def u_to_a(u):
    '''Convert unicode to ASCII, if ASCII passed in than return it
    Args:
    u -- text to be converted 
    '''    
    if type(u) is unicode:
        return normalize('NFKD', u).encode('ascii','ignore')
    elif type(u) is str:
        return u

    
def get_sargs(headline_blob, cut_off=0.5):
    sargs = []
    tfidf_scores = score_headline(headline_blob)
    #word_tags = headline_blob.tags
    #nouns = {word for word, tag in word_tags if tag in {u'NN', u'NNS'}}
    if not cut_off:
        cut_off = median([score for _, score in tfidf_scores])
    for word, score in tfidf_scores:
        if score >= cut_off: #and word in nouns:
            sargs.append((word, score))
    # if not sargs:
    #     for word, score in tfidf_scores:
    #         if score > cut_off:
    #             sargs.append((word, score))      
    return sargs  
    

def dt_to_epoch(dt):
    return (dt - datetime(1970,1,1)).total_seconds()


def get_headlines_for_ddl(dt_string):
    dt = parse(dt_string)
    start_day = dt_to_epoch(dt)
    end_day = dt_to_epoch(dt + timedelta(days=1)) 
    client = MongoClient()
    db = client.twitter_news
    news_coll = db.news
    cursor = news_coll.find({"$and":
                             [ {"time": {"$gt": start_day}}, 
                              {"time": {"$lt": end_day}}
                             ]})
    headlines = []
    for item in cursor:
        h_id = str(item['_id'])
        h_text = item['headline']
        headlines.append({'text': h_text, 'id': h_id})
    return headlines

    
def get_sargs_from_text(headline):
    h_blob = blob_headline(headline)
    sargs = get_sargs(h_blob, None)
    words = ' '.join([t[0] for t in sargs])
    return words


def get_s_score(headline):
    headline_blob = blob_headline(headline)
    s = headline_blob.sentiment
    return abs(s.polarity)


def get_headline_by_id(headline_id):    
    client = MongoClient()
    db = client.twitter_news
    news_coll = db.news
    return db.news.find_one({u'_id': ObjectId(headline_id)})
