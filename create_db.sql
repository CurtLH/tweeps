-- drop the existing database
DROP DATABASE IF EXISTS twitter;

-- recreate the database
CREATE DATABASE twitter;

-- connect to the database
\c twitter

-- create a table for the raw tweets
CREATE TABLE tweets_raw (
  id SERIAL,
  datetime_load timestamp default CURRENT_TIMESTAMP,
  tweet JSONB,
  PRIMARY KEY (id)
);
