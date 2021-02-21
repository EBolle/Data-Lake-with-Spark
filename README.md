## Data lake with Spark

Requirements
- AWS EMR -> post the AWS command line setup for exact replication
- Post the exact command line spark-submit code
- Config -> only things that needs to be adjusted is the s3 location

- why do we make certain partition decisions -> explain the impact of small data and the cost of reshuffling,
plus the tradeoff between long writes versus a clean datalake schema.
E.g., for the songs table I cut the number of partitions from 466 to 8 since the overhead is not worth it with
these small amounts of data.
- Perhaps show some graphs of skew? 
