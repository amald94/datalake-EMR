## Project Overview

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their 
data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ETL pipeline for a data lake hosted on S3. 
We will load data from S3, process the data into analytics tables using Spark, and load them back into S3. 
We will deploy this Spark process on a cluster using AWS.

## Summary
* [Project structure](#Structure)
* [Datasets](#Datasets)
* [Schema](#Schema)
* [Deployement](#Deployement)

#### Structure

* <b> /images </b> - Some screenshots.
* <b> etl.py </b> - Spark script to perform ETL job.
* <b> dl.cfg </b> - Configuration file to add AWS credentials.

## Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

* <b> Song data </b> - s3://udacity-dend/song_data
* <b> Log data </b> - s3://udacity-dend/log_data

## Schema

#### Fact Table
songplays - records in event data associated with song plays. Columns for the table:

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables 
##### users

    user_id, first_name, last_name, gender, level
##### songs

    song_id, title, artist_id, year, duration

##### artists

    artist_id, name, location, lattitude, longitude

##### time

    start_time, hour, day, week, month, year, weekday

## Deployement

* Fill your AWS Credentials in dl.clg file
* Create a EMR Cluster 

<div align="center">
<img src=images/emr.png >
<p>Create an EMR Cluster</p>
</div>

* Copy your code to emr 
* Execute etl.py as a spark-submit job

<div align="center">
<img src=images/etl.png >
<p>Execute spark job</p>
</div>

* Check the processed result in s3
<div align="center">
<img src=images/s3.png >
</div>
<div align="center">
<img src=images/s32.png >
</div>
<div align="center">
<img src=images/s33.png >
</div>
<div align="center">
<img src=images/s34.png >
</div>
