# Project 3: Sparkify Data Warehouse

## Context
A music streaming startup, Sparkify, has grown their userbase and song database and want to move their processes and data on the cloud using Amazon's Architecture.

Currently their data resides in Amazon S3, in a directory of JSON logs on useractivity taken from their app as well as a directory of JSON metadata on their songs in the app.

---

## Project Description
This project builds an ETL pipeline for a database hosted on Redshift. The pipeline extracts the song and user data from Amazon S3, stages the full data set in Redshift and the transforms the data into a set of fact and dimension tables. 

This will give the analytics team the ability to query the data warehouse in an efficient way to garner insights about the app.

---

## Getting Started
First you will need to update the credentials file with your AWS details: namely the Access Key and the Secret.

Install python3 and AWS SDK.

You will then need to do the following on AWS:
* Create clients for EC2, S3, IAM, and Redshift.
* Create an IAM user, IAM role, 
* Create a Redshift Cluster
* Extract the ARN

To do the above you will need to follow the instructions and run the Jupyter file 'Redshift - IaC.ipynb'. Note that you will only run **STEP 5: Clean up your resources** after you are finished creating your Data Warehouse. This will ensure that you won't run up unnecessary costs for running your Redshift Cluster.

Run the below files from the command line:
* 'create_tables.py'
* 'etl.py'

You can also run the jupyter notebook 'Run_me.ipynb' which is a jupyter notebook version which will run the above two files as well as explore the created tables using inline sql.

---
## The Data

### S3
To understand the data that you have on S3 and to identify which database schema to use, you can run the jupyter notebook file 'Data Viewer.ipynb' which will allow you to browse the S3 data in dataframe.

The user data has the following 18 fields:

| artist | auth | first_name | gender | item_in_session | last_name | length | level | location | method | page | registration | session_id | song | status | ts | user_agent | user_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None | Logged In | Walter | M | 0 | Frye | NaN | free | San Francisco-Oakland-Hayward, CA | GET | Home | 1540919166796 | 38 | None | 200 | 1541105830796 | "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4... | 39 |

The song data has the following 10 fields:

| artist_id | artist_latitude | artist_location | artist_longitude | artist_name | duration | num_songs | song_id | title | year | 
|---|---|---|---|---|---|---|---|---|---|
|ARVBRGZ1187FB4675A|NaN||NaN|Gwen Stefani|290.55955|1|SORRZGD12A6310DBC3|Harajuku Girls|2004|

The above data is gleaned from the following AWS S3 Buckets:
* LOG_DATA ='s3://udacity-dend/log_data'
* LOG_JSONPATH ='s3://udacity-dend/log_json_path.json'
* SONG_DATA ='s3://udacity-dend/song_data'

### Redshift Database
The data is first staged into two (bigger) tables: song_staging and event_staging which includes all fields.
The dimension and fact tables are then pulled from the 2 comprehensive staging tables to create:

**Fact Table**
* songplays: songplay_id, start_time, user_id, level, song_id, artist_id, year, duration

***Dimension Tables***
* songs: song_id, title, artist_id, year, duration
* users: user_id, first_name, last_name, gender, level
* artists: artist_id, name, location, latitude, longitude
* time: start_time, hour, day, week, month, year, weekday


For the data types in the SQL tables, to remove complexity I just consistently used 3 data types: VARCHAR, INT and BIGINT. For additional performance enhancements DISTKEYS and SORTKEYS were added to the tables.

**Table sizes**:

**Staging Tables**

| table name | no. rows |
|---|---|
|staging_songs|14896|
|staging_events|8056|

**Database Tables**

| table name | no. rows |
|---|---|
|songplays|333|
|songs|14896|
|artists|10025|
|users|104|
|time|8023|


---




