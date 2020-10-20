import json
import time
import datetime
import itertools
import pandas as pd


def load_tweets(in_file):

    """
    Loads JSON tweets_list into a list of dicts
  
    Parameters
    ----------
    N/A

    Returns
    -------
    list of dictionaries, where each tweet represents a dictionary

    Examples
    --------
    >>> tweets = load_tweets('tweets.json')
    """

    tweets = []
    for line in open(in_file):

        # check if each line is has 'user'
        if "user" in line:

            # append line to tweets
            try:
                tweets.append(json.loads(line))
            except:
                pass

    return tweets


def clean_tweets(
    tweets, clean_tweet_text=True, clean_user_description=True, convert_date=True
):

    """
    Standardizes 'created_at', removes '\t' and '\n' from ['text'] and ['user']['description']
  
    Parameters
    ----------
    convert_date : transforms Twitter date/time into standrd date/time (default is True to convert)

    Returns
    -------
    list of list of dictionaries, where each tweet represents a dictionary
    tweet text is cleaned of any '\n' or '\t', which can cause problems when parsing tweets_list

    Examples
    --------
    >>> tweets = clean_tweeets(tweets)
    
    """

    for line in tweets:

        if clean_tweet_text is True:
            # remove tabs in tweet text
            if "\t" in line["text"]:
                line["text"] = line["text"].replace("\t", "")

            # remove new line in tweet text
            if "\n" in line["text"]:
                line["text"] = line["text"].replace("\n", "")

            # remove /r in tweet text
            if "\n" in line["text"]:
                line["text"] = line["text"].replace("\n", "")

        if clean_user_description is True:
            # check if each a user description is not blank
            if line["user"]["description"] != None:

                # remove tabs in user description
                if "\t" in line["user"]["description"]:
                    line["user"]["description"] = line["user"]["description"].replace(
                        "\t", ""
                    )

                # remove /r in user description
                if "\n" in line["user"]["description"]:
                    line["user"]["description"] = line["user"]["description"].replace(
                        "\n", ""
                    )

                # remove new line in user description
                if "\r" in line["user"]["description"]:
                    line["user"]["description"] = line["user"]["description"].replace(
                        "\r", ""
                    )

        if convert_date is True:
            # format date/time
            line["CREATED_AT"] = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.strptime(line["created_at"], "%a %b %d %H:%M:%S +0000 %Y"),
            )
            dt = line["CREATED_AT"].split(" ")
            d = dt[0].split("-")
            t = dt[1].split(":")
            line["CREATED_AT_DATE_TIME"] = datetime.datetime(
                int(d[0]), int(d[1]), int(d[2]), int(t[0]), int(t[1]), int(t[2])
            )

    return tweets


def process_tweets(tweets, classify_tweet_type=True, extract_tweet_entities=True):

    """
    Classifies type of tweet (mention/reply/retweet) and extracts tweet entities. 
  
    Parameters
    ----------


    Returns
    -------
    list of list of dictionaries, where each tweet represents a dictionary

    Examples
    --------
    >>> tweets = process_tweeets(tweets)
    """

    for line in tweets:

        if classify_tweet_type is True:
            # classify tweet as retweet/mention/tweet
            if "retweeted_status" in line:
                line["TWEET_TYPE"] = "retweet"
            elif len(line["entities"]["user_mentions"]) > 0:
                line["TWEET_TYPE"] = "mention"
            else:
                line["TWEET_TYPE"] = "tweet"

        if extract_tweet_entities is True:
            # check if line contains a menetion, and if so, extract all users mentione
            tweeties = []
            line["TWEETIES"] = ""
            if len(line["entities"]["user_mentions"]) > 0:
                tweeties.extend(line["entities"]["user_mentions"])
                line["TWEETIES"] = " ".join([user["screen_name"] for user in tweeties])

            # check if line contains a hashtag, and if so, extact all hashtags
            hashtags = []
            line["HASHTAGS"] = ""
            if len(line["entities"]["hashtags"]) > 0:
                hashtags.extend(line["entities"]["hashtags"])
                line["HASHTAGS"] = " ".join([tag["text"] for tag in hashtags])

            # check if line contains a URL, and if so, extract all expanded URLS
            expanded_urls = []
            line["EXPANDED_URLS"] = ""
            if len(line["entities"]["urls"]) > 0:
                expanded_urls.extend(line["entities"]["urls"])
                line["EXPANDED_URLS"] = " ".join(
                    [url["expanded_url"] for url in expanded_urls]
                )

            # check if line has lat/long, and if so, extract lat/long
            line["LATITUDE"] = ""
            line["LONGITUDE"] = ""
            if line["geo"] is not None:
                line["LATITUDE"] = line["geo"]["coordinates"][0]
                line["LONGITUDE"] = line["geo"]["coordinates"][1]

    return tweets


def load_clean_and_processed_tweets(in_file):

    """
    Loads JSON tweets_list into a list of dicts, clean 'created_at' and 'text' fields, returns list of dicts
    
    Parameters
    ----------


    Returns
    -------


    Examples
    --------
    >>> 
    
    """

    # load raw JSON into a list of dicts using load_tweets function
    tweets = load_tweets(in_file)

    # clean ['text'], ['user']['description'], and ['created_at'] fields using clean_tweets function
    tweets_clean = clean_tweets(
        tweets, clean_tweet_text=True, clean_user_description=True, convert_date=True
    )

    # process tweets
    tweets_processed = process_tweets(
        tweets_clean, classify_tweet_type=True, extract_tweet_entities=True
    )

    return tweets_processed


def parse_raw_tweets(in_file, return_tweets_header=None):

    """
    Returns list of lists of tweets containing certain fields
    
    Parameters
    ----------


    Returns
    -------


    Examples
    --------
    
    
    """

    # load tweets from JSON and clean tweets using clean_tweets()
    tweets = load_clean_and_processed_tweets(in_file)

    # extract fields from tweet into a list of tuples
    tweets_list = []
    for line in tweets:
        row = (
            line["CREATED_AT_DATE_TIME"],  # date/time of tweet
            line["user"]["screen_name"],  # tweet author username
            line["text"],  # tweet text
            line["TWEET_TYPE"],  # tweet type (retweet/mention/tweet)
            line[
                "TWEETIES"
            ],  # usernames of those mentioned in the tweet (delimitied by a space)
            line["HASHTAGS"],  # hashtags in the tweet (delimited by a space)
            line["EXPANDED_URLS"],  # expanded URLs in the tweet (delimited by a space)
            line["lang"],  # twitter machine learning language detection algorithm
            line["user"]["id_str"],  # user's unique id
            line["user"][
                "description"
            ],  # user's self-defined description (at time of tweet)
            line["user"][
                "followers_count"
            ],  # count of a user's followers (at time of tweet)
            line["user"][
                "friends_count"
            ],  # count of a user's friends (at time of tweet)
            line["user"][
                "statuses_count"
            ],  # count of a user's total tweets (at time of tweet)
            line["LATITUDE"],  # geo location latitude, if provided
            line["LONGITUDE"],  # geo location longitude, if provided
        )
        tweets_list.append(row)

    # define header
    header = [
        "CREATED_AT_DATE_TIME",
        "USER_SCREEN_NAME",
        "TEXT",
        "TWEET_TYPE",
        "TWEETIES",
        "HASHTAGS",
        "EXPANDED_URLS",
        "LANGUAGE",
        "USER_ID_STR",
        "USER_DESCRIPTION",
        "USER_FOLLOWERS_COUNT",
        "USER_FRIENDS_COUNT",
        "USER_STATUSES_COUNT",
        "LONGITUDE",
        "LATITUDE",
    ]

    if return_tweets_header is True:

        # create header row to write to CSV file
        return tweets_list, header

    else:

        # zip tweets into a list of dicts
        tweets_dict = [dict(zip(header, line)) for line in tweets_list]

        return tweets_dict


def tweets_to_csv(in_file):

    """
    Writes tweets to CSV
    
    Parameters
    ----------
    

    Returns
    -------


    Examples
    --------
    >>> 
    
    """

    # create tweets edgelist
    tweets_dict = parse_raw_tweets(in_file, return_tweets_header=None)

    # convert list of tuples to pandas dataframe
    tweets_df = pd.DataFrame(tweets_dict)

    # create out_file name
    if in_file.endswith(".txt"):
        out_file = in_file.replace(".txt", "_processed.csv")
    elif in_file.endswith(".json"):
        out_file = in_file.replace(".json", "_processed.csv")
    else:
        out_file = in_file + str("_processed.csv")

    # write tweets_df to CSV
    tweets_df.to_csv(out_file, index=False, encoding="utf-8")


def most_recent_user_stats(in_file, return_tweets_dict=None):

    """
    Get most user-based statistics from their most recent tweet 
    
    Parameters
    ----------
    

    Returns
    -------


    Examples
    --------
    >>> 
    
    """

    # load parsed tweets_list into a list of tuples and zip into a list of dicts
    tweets_dict = parse_raw_tweets(in_file, return_tweets_header=False)

    # extract user data into a new list
    user_data = []
    for line in tweets_dict:
        row = (
            line["CREATED_AT_DATE_TIME"],
            line["USER_SCREEN_NAME"],
            line["USER_ID_STR"],
            line["USER_DESCRIPTION"],
            line["USER_FOLLOWERS_COUNT"],
            line["USER_FRIENDS_COUNT"],
            line["USER_STATUSES_COUNT"],
        )
        user_data.append(row)

    # extract user data from retweets

    # sorted user data by screenname and datetime
    user_data_sorted = sorted(user_data, key=lambda x: (x[1], x[0]), reverse=True)

    # group by username and get the most recent user data
    most_recent_user_data = [
        (lambda x: (x[1], x[2], x[3], x[4], x[5], x[6]))(next(v))
        for k, v in itertools.groupby(user_data_sorted, key=lambda x: x[1])
    ]

    if return_tweets_dict is True:
        return most_recent_user_data, tweets_dict

    else:
        return most_recent_user_data


def format_as_edgelist(in_file, return_tweets_header=None):

    """
    Formats data as an edgelist with most recent user data for all of a user's tweets
    
    Parameters
    ----------
    

    Returns
    -------


    Examples
    --------
    >>> 
    
    """

    ## load most recent user data, tweets_list, and header
    most_recent_user_data, tweets_dict = most_recent_user_stats(
        in_file, return_tweets_dict=True
    )

    ## pasrse tweets_dict to create a single relationship b/t USER_SCREEN_NAME and each user in the TWEETIES field

    # create an empty list to append transformed edgelist to
    tweets_edgelist = []

    # iterate through each line in tweets_dict
    for line in tweets_dict:

        # if there are no users in the tweeties field, create a self-loop
        if line["TWEETIES"] == "":

            # append modified line to tweets_edgelist
            row = (
                line["CREATED_AT_DATE_TIME"],
                line["USER_SCREEN_NAME"],
                line["TEXT"],
                line["TWEET_TYPE"],
                line["TWEETIES"],
                line["HASHTAGS"],
                line["EXPANDED_URLS"],
                line["LANGUAGE"],
                line["USER_ID_STR"],
                line["USER_DESCRIPTION"],
                line["USER_FOLLOWERS_COUNT"],
                line["USER_FRIENDS_COUNT"],
                line["USER_STATUSES_COUNT"],
                line["LONGITUDE"],
                line["LATITUDE"],
                line["USER_SCREEN_NAME"],
                str(""),
            )

            tweets_edgelist.append(row)

        # if the tweeties field is not blank, create a new line between USER_SCREEN_NAME and each user in the TWEETIES field
        else:

            # split the tweeties field into a split
            tweeties = line["TWEETIES"].split()

            # loop through each of the usernames in the users variable
            for user in tweeties:

                # append modified line to tweets_edgelist
                row = (
                    line["CREATED_AT_DATE_TIME"],
                    line["USER_SCREEN_NAME"],
                    line["TEXT"],
                    line["TWEET_TYPE"],
                    line["TWEETIES"],
                    line["HASHTAGS"],
                    line["EXPANDED_URLS"],
                    line["LANGUAGE"],
                    line["USER_ID_STR"],
                    line["USER_DESCRIPTION"],
                    line["USER_FOLLOWERS_COUNT"],
                    line["USER_FRIENDS_COUNT"],
                    line["USER_STATUSES_COUNT"],
                    line["LONGITUDE"],
                    line["LATITUDE"],
                    line["USER_SCREEN_NAME"],
                    user,
                )

                tweets_edgelist.append(row)

    if return_tweets_header is True:
        header = [
            "CREATED_AT_DATE_TIME",
            "USER_SCREEN_NAME",
            "TEXT",
            "TWEET_TYPE",
            "TWEETIES",
            "HASHTAGS",
            "EXPANDED_URLS",
            "LANGUAGE",
            "USER_ID_STR",
            "USER_DESCRIPTION",
            "USER_FOLLOWERS_COUNT",
            "USER_FRIENDS_COUNT",
            "USER_STATUSES_COUNT",
            "LONGITUDE",
            "LATITUDE",
            "VERTEX_1",
            "VERTEX_2",
        ]

        return tweets_edgelist, header

    else:
        return tweets_edgelist


def edgelist_to_csv(in_file):

    """
    Writes edgelist to CSV
    
    Parameters
    ----------
    

    Returns
    -------


    Examples
    --------
    >>> 
    
    """

    # create tweets edgelist
    tweets_edgelist, header = format_as_edgelist(in_file, return_tweets_header=True)

    # convert list of tuples to pandas dataframe
    edgelist_df = pd.DataFrame(tweets_edgelist, columns=header)

    # create out_file name
    if in_file.endswith(".txt"):
        out_file = in_file.replace(".txt", "_edgelist..csv")
    elif in_file.endswith(".json"):
        out_file = in_file.replace(".json", "_edgelist.csv")
    else:
        out_file = in_file + str("_edgelist.csv")

    # write tweets_df to CSV
    edgelist_df.to_csv(out_file, index=False, encoding="utf-8")
