# Portfolio Hands-On AWS

This repository contains a collection of hands-on projects focused on data engineering using AWS services. The goal is to apply key concepts in real-world scenarios and demonstrate technical skills in cloud architecture, data automation, and best practices.

## Goals

- Consolidate knowledge of AWS services applied to data engineering;
- Build a portfolio of real-world projects for use in job interviews and technical assessments;
- Gradually evolve into more complex architectures using Lambda, Glue, S3, Athena, Step Functions, and other tools;
- Share practical learning experiences with other data professionals and learners.

## Projects

1. [CSV to Parquet with AWS Lambda](./csv-to-parquet-lambda)  
   Automatically converts CSV files stored in the *landing* layer into Parquet files in the *clean* layer using AWS Lambda, S3, and Glue Catalog.

2. [Automated data partitioner with AWS Lambda](./s3-parquet-partitioning-lambda)
   Automated data pipeline that processes non-partitioned Parquet files from S3 landing zone, intelligently detects date columns, and creates partitioned datasets in the clean zone.
   
(More projects coming soon...)
