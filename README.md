# Data lake with Spark

After moving to the powerful AWS Redshift datawarehouse in the cloud, one could wonder why we want to switch to Spark. 
Currently, the main reason is that Sparkify is planning to start using advanced analytics and machine learning. While
Redshift is very powerful and useful for fast querying and feeding dashboards, it is not designed for advanced analytics
or machine learning. Spark on the other hand, has several machine learning API's capable of training models in parallel
on massive amounts of data.

To get acquainted with Spark as fast as possible we have decided to run most of the ETL jobs on Spark clusters, and this
project is an example of those.

## ETL

This project **extracts** raw .json data from S3, **transforms** it with Spark on a AWS EMR cluster, and **loads**
it back in S3 in parquet format, in detail:

- Raw .json song data is transformed into 2 tables - songs and artists - in parquet format
- Raw .json log data is transformed into 3 tables - time, users, and songplays - in parquet format

## Requirements

To replicate the project you need to activate 3 AWS services / tools:

- AWS CLI version 2, more information can be found [here][cli_version_2]
- An EMR cluster with Spark installed and configured
- An S3 bucket

To avoid cross-region data transfer fees, make sure your S3 bucket is in the same regio as the EMR cluster, more 
information can be found [here][s3_info].

This project was tested and validated with the following EMR settings:

```bash
aws emr create-cluster \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Spark \
--ebs-root-volume-size 10 \
--service-role EMR_DefaultRole \
--ec2-attributes KeyName=<your permission key name> \
--release-label emr-5.32.0 \
--name 'spark-cluster' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2
```

## Instructions

Once your cluster is running, you need to do a couple of things to actually start the ETL process:

- modify the S3 bucket location in dl.cfg
- upload dl.cfg, etl.py, and schemas.py to a folder on a S3 bucket of your choice
- make sure you add an inbound security rule to allow ssh access to the cluster on your local ip address ([more info][ssh_master_node])
- connect to the cluster via a private key pair

```bash
ssh -i <location to your .pem file> hadoop@<master-public-dns-name>
```
- create a folder and move into the folder

```bash
mkdir spark_app
cd spark_app
``` 

- sync the S3 folder with the scripts

 ```bash
 aws s3 sync s3://<your bucket>/<your folder with scripts> .
```

- execute etl.py

```bash
/usr/bin/spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true etl.py
```

If you want to track and inspect the progress and jobs via the Spark-UI, you have to set up an ssh-tunnel in a new
terminal as follows:

```bash
aws emr socks --cluster-id <your_cluster_id> --key-pair-file <location to your .pem file>
```

Next, navigate to `http://<master-public-dns-name>:18080` and follow the action. Note that you have to configure your
browser proxy settings as well, more information can be found [here][emr_ssh_tunnel] and [here][emr_master_node].

## Small data

Please note that the current input data on the S3 buckets is *very* small in size. This is due to the fact that this
project is part of an educational program. As a consequence, some of the 'optimizations' in the code do not make sense
given the small amount of data, but should provide better performance in case of larger amounts of data, e.g.:

- repartitioning to a smaller number of partitions than default to avoid overhead for future actions
- repartition songs on year and artist_id to create a more useful parquet schema for further usage, although it takes
more time to write the files when using this partition strategy
- caching the 2 input dataframes since they are used more than once and fit into memory easily
- only repartition based on columns when necessary, in other cases the current partitions are used to write to S3
more efficiently
- broadcast the smaller songs_df dataframe to each executor when joining with the events_df dataframe to avoid shuffling

## Contact

In case of suggestions or remarks please contact the Data Engineering department.

[cli_version_2]: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
[s3_info]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-region.html
[ssh_master_node]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html
[emr_ssh_tunnel]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel.html
[emr_master_node]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html
