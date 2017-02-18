# Welcome to my tweeps!

Check out the [develop branch](https://github.com/CurtLH/tweeps/tree/develop)
for the latest updates...

The tweeps application provides a simple way to connect to the the Twitter Streaming API.  After you submit your keywords and hashtags to tweeps, the application will maintain a constant connection to the API, and store new tweets in a PostgreSQL database as they come into the system.  The application also includes an ETL process that will regularily monitor the source database and routinely extract, transform, and load data from the source database into a data warehouse. 
