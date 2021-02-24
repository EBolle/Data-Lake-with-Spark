## Data lake with Spark

<img src="https://images.pexels.com/photos/4432160/pexels-photo-4432160.jpeg?auto=compress&cs=tinysrgb&dpr=2&h=650&w=940" height="400">

Source: https://www.pexels.com

### Why Spark?

After moving to the powerful AWS Redshift datawarehouse in the cloud, one could wonder why we want to switch to Spark. 
Currently, the main reason is that Sparkify is planning to start using advanced analytics and machine learning. While
Redshift is very powerful and useful for fast querying and feeding dashboards, it is not designed for advanced analytics
or machine learning. Spark on the other hand, has several machine learning API's capable of training models in parallel
on massive amounts of data.

To get acquainted with Spark as fast as possible we have decided to run most of the ETL jobs on Spark clusters, and this
project is an example of those.

### ETL

This project *extracts* raw .json data from S3, *transforms* it on a Spark cluster running on AWS EMR, and *loads* it
in S3 in parquet format, in detail:

- Raw .json song data is transformed into 2 folders - songs and artists - in parquet format
- Raw .json log data is transformed into 3 folders - time, users, and songplays - in parquet format

### Requirements

To replicate the project you need to activate 2 AWS services:
- Analytics: EMR with Spark installed and configured
- Storage: S3

This project was tested and validated with the following EMR settings:

```bash
EMR code
```

How to get the code on EMR -> add this project? or copy? 

### Run

Once your Spark cluster is up and running you 

Requirements
- AWS EMR -> post the AWS command line setup for exact replication
- Post the exact command line spark-submit code
- Config -> only things that needs to be adjusted is the s3 location

- why do we make certain partition decisions -> explain the impact of small data and the cost of reshuffling,
plus the tradeoff between long writes versus a clean datalake schema.
E.g., for the songs table I cut the number of partitions from 466 to 8 since the overhead is not worth it with
these small amounts of data.
- Perhaps show some graphs of skew? 
- Why use cache? 

Explain the songs_df return / function parameter -> events_df as help table etc.
