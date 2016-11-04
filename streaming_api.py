#!/usr/bin/env python

import logging
import time
import csv
import json
import oauth_creds as oauth
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from datetime import datetime
from dateutil import parser


# enable logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', 
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# authorize the app to access Twitter on our behalf
auth = OAuthHandler(oauth.consumer_key, oauth.consumer_secret)
auth.set_access_token(oauth.access_token, oauth.access_secret)
api = tweepy.API(auth)


# establish open connection to streaming API
class MyListener(StreamListener):
    
    def on_data(self, data):
        try:
            tweet = parse_tweet(data)
            content = extract_content(tweet)
            with open('tweets.csv', 'a') as f: 
                writer = csv.writer(f, quotechar = '"')
                writer.writerow(content)
                #logger.info(content[3])

        except BaseException as e:
            logger.warning(e)
        
        return True
        
    def on_error(self, status):
        logger.warning(status)
        return True


# parse data
def parse_tweet(data):
    
    # load JSON item into a dict
    tweet = json.loads(data)
    
    # check if tweet is valid
    if 'user' in tweet.keys():  
        tweet['CREATED_AT'] = parser.parse(tweet['created_at'])
        if 'retweeted_status' in tweet:
            tweet['TWEET_TYPE'] = 'retweet'
        elif len(tweet['entities']['user_mentions']) > 0:
            tweet['TWEET_TYPE'] = 'mention'
        else:
            tweet['TWEET_TYPE'] = 'tweet'
        return tweet
    else:
        #logger.warning("Incomplete tweet: %s", tweet)
        pass


# extract relevant data to write to CSV
def extract_content(tweet):
    
    content = [tweet['user']['screen_name'], 
               tweet['CREATED_AT'].strftime('%Y-%m-%d %H:%M:%S'),
               tweet['TWEET_TYPE'],
               tweet['text'].encode('unicode_escape')]
    
    return content    


def start_stream():
   
    terms = list([line.strip() for line in open('terms.txt')])
    
    while True:
        
        logger.warning("Twitter API Connection opened")
        
        try:
            twitter_stream = Stream(auth, MyListener())
            twitter_stream.filter(track=terms)
        
        except Exception as e: 
            logger.warning(e)
            continue
        
        finally:
            logger.warning("Twitter API Connection closed")


if __name__ == '__main__':
    start_stream()
