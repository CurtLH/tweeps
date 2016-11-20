#!/usr/bin/env python

import logging
import json
import oauth_creds as oauth
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


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
            tweet = json.loads(data)
            with open('tweets.json', 'a') as f:
                json.dump(tweet, f)
                f.write('\n')

        except BaseException as e:
            logger.warning(e)

        return True

    def on_error(self, status):
        logger.warning(status)
        return True


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
