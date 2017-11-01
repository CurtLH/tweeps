#!/usr/bin/env python

import click
import logging
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import psycopg2
from datetime import datetime


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

# enable autocommit
conn.autocommit = True

# define the cursor to be able to write to the database
cur = conn.cursor()

# read in api tokens
with open('/home/curtis/etc/twitter') as f:
    token = json.load(f)

# authorize the app to access Twitter on our behalf
auth = OAuthHandler(token['consumer_key'], token['consumer_secret'])
auth.set_access_token(token['access_token'], token['access_token_secret'])
api = tweepy.API(auth)

# load collection terms
terms = list([line.lower().strip() for line in open('./tweeps/terms.txt')])

# create table if it doesn't exists
cur.execute("""CREATE TABLE IF NOT EXISTS twitter_raw 
               (id SERIAL PRIMARY KEY NOT NULL, 
               tweet JSONB)""")   

# establish open connection to streaming API
class MyListener(StreamListener):

    def on_data(self, data):
        try:
            if 'user' in data:

                # load tweet into a dict
                tweet = json.loads(data)

                # identify matching collection term
                tweet['TERMS'] = [term for term in terms if term in tweet['text'].lower()]                

                # identify collection datetime
                tweet['COLLECTED_AT'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # insert tweet into database
                if len(tweet['TERMS']) > 0:
                    cur.execute("INSERT INTO twitter_raw (tweet) VALUES (%s)", [json.dumps(tweet)])

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


@click.command()
def cli():

    """Start the Twitter Streaming API"""

    start_stream()


if __name__ == '__main__':
    cli()
