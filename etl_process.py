#!/usr/bin/env python

import logging
import psycopg2
from dateutil import parser
from datetime import datetime


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_datetime(datetime):

    return parser.parse(datetime)


def classify_tweet(tweet):

    if 'retweeted_status' in tweet:
        tweet['TWEET_TYPE'] = 'retweet'
    elif len(tweet['entities']['user_mentions']) > 0:
        tweet['TWEET_TYPE'] = 'mention'
    else:
        tweet['TWEET_TYPE'] = 'tweet'

    return tweet


##### MAIN PROGRAM ####

def load_data():

    # connect to the database
    try:
        conn = psycopg2.connect(database="postgres",
                                user="postgres",
                                password="apassword",
                                host="localhost")
        conn.autocommit = True
        logger.info("Conncted to database")

    except:
        logger.warning("Cannot connect to database")

    # define the cursor to be able to query the database
    cur = conn.cursor()

    # query the database and store the results in a list a list of dicts
    cur.execute("SELECT tweet FROM twitter2")
    tweets = [record[0] for record in cur]
    logger.warning("%s tweets loaded from OLTP", len(tweets)) 

    # load transformed tweets into the database
    for line in tweets:

        # parse datetime
        line['CREATED_AT'] = parse_datetime(line['created_at'])  
    
        # classify tweet by type
        classify_tweet(line)

        # insert into database
        cur.execute("INSERT INTO twitter3 (created_at, screen_name, text) VALUES (%s, %s, %s )", [line['CREATED_AT'], line['user']['screen_name'], line['text']])

    logger.warning("tweets successfully loaded into OLAP")

if __name__ == "__main__":
    load_data()
