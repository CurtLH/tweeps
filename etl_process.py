#!/usr/bin/env python

import click
import tweepy_functions as tf
from time import sleep


# enable logging
logger = tf.enable_logger()


##### MAIN PROGRAM ####

@click.command()
def cli():

    """ETL process for Twitter data"""

    # connect to the database
    try:
        cur = tf.connect_to_db()
        logger.info("Successfully connected to database")

    except:
        logger.warning("Cannot connect to database")
    
    # create table if it doesn't exists 
    cur.execute("""CREATE TABLE IF NOT EXISTS twitter_1 ( 
                   id SERIAL PRIMARY KEY NOT NULL,
                   id_str VARCHAR NOT NULL UNIQUE,
                   created_at TIMESTAMP,
                   screen_name VARCHAR, 
                   tweet_type VARCHAR,
                   text VARCHAR);""")


    while True:
   
        # check the id in the last row of the source database
        cur.execute("SELECT MAX(id) FROM twitter_raw")
        from_n = cur.fetchone()[0]

        # check the id in the last row of the target database
        cur.execute("SELECT MAX(id) FROM twitter_1")
        to_n = cur.fetchone()[0]

        # if there are no records in the target data, set equal to 0 instead of none
        if to_n == None:
            to_n = 0

        # if the difference in the number of rows is greater than 500...
        if from_n - to_n >= 500:

            # define start and end position
            start_pos = to_n
            end_pos = start_pos + 500

            # get the first 500 records that have not been processed
            cur.execute("SELECT tweet FROM twitter_raw WHERE id BETWEEN %s AND %s" %(start_pos + 1, end_pos))
            tweets = [record[0] for record in cur]

            # transform tweet
            for line in tweets:
                
                # classify tweet type
                line['TWEET_TYPE'] = tf.classify_tweet(line)

                # transform datetime
                line['CREATED_AT'] = tf.parse_datetime(line['created_at'])
    

                # extract the relevant fields
                tweet = (line['id_str'],
                         line['CREATED_AT'],
                         line['user']['screen_name'],
                         line['TWEET_TYPE'],
                         line['text'])
    
                # insert into database
                try:
                    cur.execute("INSERT INTO twitter_1 (id_str, created_at, screen_name, tweet_type, text) VALUES (%s, %s, %s, %s, %s)", [item for item in tweet])
                    logger.info("Successfully loaded record into twitter")
                    sleep(2)

                except:
                    logger.warning("Cannot load record into twitter")
                    sleep(2)
                    pass

        else:

            # wait for a minute they try again
            sleep(60)


if __name__ == "__main__":
    cli()
