#!/usr/bin/env python

import logging
import json
import oauth_creds as oauth
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import psycopg2


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# connect to the tweets database
conn = psycopg2.connect(database="postgres",
                        user="postgres",
                        password="apassword",
                        host="localhost")

# define the cursor to be able to write to the database
cur = conn.cursor()

# authorize the app to access Twitter on our behalf
auth = OAuthHandler(oauth.consumer_key, oauth.consumer_secret)
auth.set_access_token(oauth.access_token, oauth.access_secret)
api = tweepy.API(auth)

# load collection terms
terms = list([line.lower().strip() for line in open('terms.txt')])

# establish open connection to streaming API
class MyListener(StreamListener):

    def on_data(self, data):
        try:
            if 'user' in data:

                tweet = json.loads(data)
                tweet['TERMS'] = [term for term in terms if term in tweet['text'].lower()]                

                if len(tweet['TERMS']) > 0:
                    cur.execute("INSERT INTO twitter2 (tweet) VALUES (%s)", [json.dumps(tweet)])
                    conn.commit()

            else:
                logger.warning(data)
                pass

        except BaseException as e:
            logger.warning(e)

        return True

    def on_error(self, status):
        logger.warning(status)
        return True


def start_stream():

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
