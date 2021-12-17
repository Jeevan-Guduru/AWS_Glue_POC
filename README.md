# Reddit_Hot_posts_ETL
This repository is built to host the code scripted for Pelago coding challenge.

# Requirement:
Build an ETL pipeline that fetches top 100 hot posts from API (reddit) on an hourly basis and store the required fields in a DB after transformations applied whereever needed.

**Note**:  
The code above is documented with comments for each and every step which will explain the functionality implemented.

# Architecture:

Entire architecture for this solution is hosted on AWS.
Used below AWS services to build this solution:
* Glue
* RDS MySQL DB
* A VPC with Private/Public subnets is created where in, 
    - Glue jobs are hosted in private subnet.(A NAT Gate way and VPC endpoints are created for this subnet to access s3 buckets and RDS instance.)
    - RDS is in public subnet. (for end user to view the data.)
* s3 buckets are created as needed for hosting the scripts and staging data.
    - Staging bucket : reddit-hot-posts-staging
    - Job Scripts: aws-glue-scripts-229313215368-us-west-2

## Architecture Diagram:   
![image](https://user-images.githubusercontent.com/64266006/146005358-69bf1ed2-855d-4b42-8b80-bd3375222434.png)


# MySQL Schema:
* DB called 'Reddit' is create and inisde which 2 tables are created as final tables. (Please refer DDLs from SQL folder).   
      - hot_posts.   
      - posts_treatment_tags.
      
  *Note*: List column 'treatment_tags' is exploded and stored in separate table along with post id.

# Code Structure

## Glue Jobs:
* Extract_Job.py:  
    This job as name suggests extracts the data from source. It connects to reddit using PRAW API.
    Fetches the required columns and further writes this data to S3 in CSV format.   
    Note:   
    1. While storing data to s3, replaced empty lines with (ctlr) for readability purposes. This is reversed while loading it into the DB.    
    2. Required libraries which are not native to Glue and required for this job are maintained in s3 bucket as .whl file.
* Transform_Load_Posts.py:  
     This job reads the data from Catalogue tables (created using crawlers). Applies required transformations and stores the data in MySQL DB in RDS.
     Treatment_tags which are lists are not processed by this job.(they are processed with below job and stored in a different table.)
* Transform_Load_Tags.py:
      This job processes the treatment_tags - it explodes the column and stores it in a different table mapping tags with post id.

## Crawlers:
  * S3_CSV_Crawler:   
      This crawler gathers the metadata from the Staging area i.e., above stored CSV S3 files and creates a table in Glue Database.
  * RDS_MySQL_Crawler:
       This crawler gathers the metadata from the target DB which is RDS MySQL here and creates 2 tables in Glue Database.

## Triggers:
  Triggers are used in this pipeline to shchedule and run the above ETL jobs.Below are the 3 triggers used:
  * Reddit_Hot_Posts_ETL - this is a scheduled trigger which triggers Extract job on hourly basis. 
  * Crawler Trigger - Event based, which triggers crawlers, once Extract job is completed.
  * Transform_Load_Trigger - Event based, which triggers transform and load jobs, once crawling is completed.

## Configuration:   
   Details required to access reddit website along with sub_reddit and staging bucket are stored in config.json file.   
   This can be modified as and when required.
       
# Work-flow   
Below is the workflow diagram :   
![image](https://user-images.githubusercontent.com/64266006/146012444-ae73dcf4-29df-411e-aa1d-31479ad9324f.png)




__Note__: Minimal manual intervention is needed to run this pipeline. Configurations can be changed when required by modifying config.json file.









