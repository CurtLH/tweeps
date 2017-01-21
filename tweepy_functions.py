#!/usr/bin/env python

import logging
import psycopg2
from dateutil import parser
from datetime import datetime


def enable_logger():

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger


def connect_to_db(database="postgres", user="postgres", password="apassword", host="localhost"):

    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host)
    conn.autocommit = True
    cur = conn.cursor()

    return cur


def parse_datetime(datetime):

    return parser.parse(datetime)


def classify_tweet(tweet):

    if 'retweeted_status' in tweet:
        return  'retweet'
    elif len(tweet['entities']['user_mentions']) > 0:
        return 'mention'
    else:
        return 'tweet'
