import feedparser
from pymongo import MongoClient
import time 
from nltk import stem


def last_headline(source, news_coll):
    if news_coll.find({'source': source}).count() > 0:
        cursor = news_coll.find({'source': source})
        last = cursor.sort('time', -1).next()
        return last['time']
    
    
def request_rss():
    client = MongoClient()
    db = client.twitter_news
    news_coll = db.news
    stemmer = stem.porter.PorterStemmer()
    rss= {'nyt': 'http://rss.nytimes.com/services/xml/rss/nyt/World.xml',
         'reuters': 'http://feeds.reuters.com/Reuters/worldNews'}
    for source in rss:
        feed = feedparser.parse(rss[source])
        last_time = last_headline(source, news_coll)
        for entry in feed['entries']:
            publish_time = time.mktime(entry['published_parsed'])
            if last_time < publish_time:
                data = {}
                data['source'] = source
                data['headline'] = entry['title']
                data['headline_bag'] = [stemmer.stem(word) for word in entry['title'].split(' ')]
                data['time'] = publish_time
                data['summary'] = entry['summary_detail']
                news_coll.insert(data)


def remove_old_news():
    client = MongoClient()
    db = client.twitter_news
    news_coll = db.news
    week_ago = time.time() - 3600 * 24 * 7
    news_coll.remove({'time': {'$lte': week_ago}})