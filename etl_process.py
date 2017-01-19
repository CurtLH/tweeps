#!/usr/bin/env python

import click
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

@click.command()
def cli():

    """ETL process for Twitter data"""

    # connect to the database
    try:
        conn = psycopg2.connect(database="postgres",
                                user="postgres",
                                password="apassword",
                                host="localhost")
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Successfully connected to the database")

    except:
        logger.warning("Cannot connect to database")

    
    # create table if it doesn't exists 
    cur.execute("""CREATE TABLE IF NOT EXISTS twitter ( 
                   id SERIAL PRIMARY KEY NOT NULL,
                   id_str VARCHAR NOT NULL UNIQUE,
                   created_at TIMESTAMP,
                   screen_name VARCHAR, 
                   tweet_type VARCHAR,
                   text VARCHAR);""")


    # query the database and store the results
    try:
        cur.execute("SELECT tweet FROM twitter_raw")
        tweets = [record[0] for record in cur]
        logger.info("Loaded %s records from twitter_raw", len(tweets)) 

    except:
        logger.warning("Unable to query twitter_raw")


    # load transformed tweets into the database
    for line in tweets:

        # parse datetime
        line['CREATED_AT'] = parse_datetime(line['created_at'])  
    
        # classify tweet by type
        classify_tweet(line)

        # extract the relevant fields
        tweet = (line['id_str'],
                 line['CREATED_AT'],
                 line['user']['screen_name'],
                 line['TWEET_TYPE'],
                 line['text'])

        # insert into database
        try:
            cur.execute("INSERT INTO twitter (id_str, created_at, screen_name, tweet_type, text) VALUES (%s, %s, %s, %s, %s)", [item for item in tweet])

        except:
           #logger.warning("Cannot load record into twitter")
            pass


    logger.info("Successfully loaded records in twitter")

if __name__ == "__main__":
    cli()
