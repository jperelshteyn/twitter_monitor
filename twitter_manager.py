from __future__ import division
from dateutil.parser import parse
from tweepy import OAuthHandler, API, TweepError
import cnfg
from pymongo import MongoClient
from pymongo import DESCENDING
from textblob import TextBlob as tb
from numpy import mean
from time import mktime
from math import floor
from bson.objectid import ObjectId
from dateutil.parser import parse
import headline_manager


client = MongoClient()
db = client.twitter_news



class Tweet:
    def __init__(self, tweet_obj, headline_obj):
        assert type(tweet_obj) is dict

        self.text = tweet_obj[u'tweet_data'][u'text']
        self.time_since = self._get_tweet_time(tweet_obj) - headline_obj[u'time']
        t_blob = tb(self.text)
        self.sentiment =  t_blob.sentiment.polarity
        self.is_valid = self._is_valid(headline_obj)
        self.scaled_time = None

    def _get_tweet_time(self, tweet_obj):
        dt = parse(tweet_obj[u'tweet_data'][u'created_at'])
        return headline_manager.dt_to_epoch(dt.replace(tzinfo=None))

    def _is_valid(self, headline_obj):
        '''
        Not valid if tweet text is exact match with headline text or was tweeted prior to headline
        '''

        if self.time_since < 0:
            return False

        if 'http' in self.text:
            stripped_text = self.text[:self.text.index('http')]
            return stripped_text.lower().strip() != headline_obj['headline'].lower().strip()

    def scale(self, denominator):
        self.scaled_time = int(floor(self.time_since / denominator))


class Graph:
    def __init__(self, tweet_objs, headline_id):
        assert type(tweet_objs) is list
        
        headline_obj = db.news.find_one({u'_id': ObjectId(headline_id)})
        self.tweets = []
        self.max_time = 0
        self.min_time = 0

        for tweet_obj in tweet_objs:
            tweet = Tweet(tweet_obj, headline_obj)
            if tweet.is_valid:
                self.tweets.append(tweet)
                self.max_time = self.max_time if tweet.time_since < self.max_time else tweet.time_since
                self.min_time = self.min_time if tweet.time_since > self.min_time else tweet.time_since

        self.time_scale, self.denominator = self._time_scale()
        self.points = self._make_points()
        self.tweet_count = len(self.tweets)

        self._sort()

    def _sort(self):
        self.tweets.sort(key=lambda tweet: tweet.time_since)

    def _make_points(self):
        points = dict()
        for tweet in self.tweets:
            tweet.scale(self.denominator)
            point = points.get(tweet.scaled_time, dict())
            point_tweets = point.get('tweets', [])
            point_tweets.append(tweet.text)
            tweet_count = len(point_tweets)
            sentiment = (point.get('sentiment', 0) * (tweet_count - 1) + tweet.sentiment) / tweet_count
            points[tweet.scaled_time] = {'tweets': point_tweets, 'sentiment': sentiment}
        return points

    def to_json(self):
        json = []
        for time, point in sorted(self.points.items(), lambda t, _: t[0]):
            json.append({'time': time,
                         'sentiment': point['sentiment'],
                         'tweets': point['tweets']})
        return json


    def _time_scale(self):
        hour = 3600
        diff_hours = (self.max_time - self.min_time) / hour
        if diff_hours > 24 * 6:
            return 'days', hour * 24
        elif diff_hours > 4:
            return 'hours', hour
        else:
            return 'minutes', hour / 60

    def __str__(self):
        return '\n'.join([' '.join(map(unicode, [t.time_since, t.scaled_time, t.text])) for t in self.tweets])




def initialize_api():
    '''
    Return tweepy api object loaded with twitter keys and tokens
    '''
    config = cnfg.load(".twitter_config")
    auth = OAuthHandler(config["consumer_key"], config["consumer_secret"])
    auth.set_access_token(config["access_token"], config["access_token_secret"])
    api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

    
def query(sarg, headline_id, max_tweets=1000, tweets_per_qry=100, max_id=-1L, since_id=None):
    '''
    Query twitter continuously with sargs and save to database

    Args:
        sarg(str) -- query search terms
        headline_id(str) --  foreign key to the news collection
        max_tweets(int) -- maximum number of tweets to collect
        tweets_per_qry(int) -- maximum number of tweets per request
        max_id(long) -- top id in range of tweets to be collected (for windowing the requests)
        since_id(long) -- bottom id in range of tweets to be collected (comes from local db)
    '''
    api = initialize_api()
    tweet_count = 0
    client = MongoClient()
    db = client.twitter_news
    tweet_coll = db.tweets
    # get tweets from db and figure out the starting id for new twitter query
    saved_tweets = tweet_coll.find({'$and' :[{'news_id': headline_id}, {'sarg': sarg}]})
    saved_ids = {}
    if saved_tweets.count() > 0:
        saved_ids = {long(c[u'tweet_data'][u'id_str']) for c in saved_tweets}
        since_id = max(saved_ids)
    else:
        since_id = find_latest_tweet_id_before_headline(headline_id)
    # request tweets until max is reached or sarg is exhausted
    while tweet_count < max_tweets:
        try:
            if (max_id <= 0):
                if (not since_id):
                    new_tweets = api.search(q=sarg, count=tweets_per_qry, lang='en')
                else:
                    new_tweets = api.search(q=sarg, count=tweets_per_qry, lang='en',
                                            since_id=since_id)
            else:
                if (not since_id):
                    new_tweets = api.search(q=sarg, count=tweets_per_qry, lang='en',
                                            max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=sarg, count=tweets_per_qry, lang='en',
                                            max_id=str(max_id - 1),
                                            since_id=since_id)
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                if not long(tweet._json[u'id_str']) in saved_ids:
                    data = {}
                    data['news_id'] = headline_id
                    data['sarg'] = sarg
                    data['tweet_data'] = tweet._json
                    tweet_coll.insert(data)
            tweet_count += len(new_tweets)
            print("Downloaded {0} tweets".format(tweet_count))
            max_id = new_tweets[-1].id
        except TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break 


def find_latest_tweet_id_before_headline(headline_id):
    '''
    Query db for tweets that came out after the headline

    Args:
        news_id(ObjectId) -- headline id

    '''
    headline = headline_manager.get_headline_by_id(headline_id)
    h_time = headline['time']
    client = MongoClient()
    db = client.twitter_news
    db.tweets.ensure_index([("tweet_data.created_at", DESCENDING)])
    cursor = db.tweets.find().sort([('tweet_data.created_at', DESCENDING)])
    for tweet in cursor:
        dt = parse(tweet[u'tweet_data'][u'created_at'])
        tweet_time = mktime(dt.timetuple())
        if tweet_time < h_time:
            return tweet['tweet_data'][u'id']


def read_db_tweets(news_id, sarg):
    '''
    Get tweets from database

    Args:
        news_id(ObjectId) -- headline id
        sarg(str) -- tweeter search arguments
    '''
    cursor = db.tweets.find({'$and' :[{'news_id': news_id}, {'sarg': sarg}]})
    return [tweet for tweet in cursor]



