import datetime
import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F


"""
Description:

Fumctionality is to reads data from source, 
apply transformations needed and stores to target Db without duplicates.

It ignores list column 'treatment_tags' as it is handled in another job.

Note: This script is built upon boilerplate code generated from Glue.

"""

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
# Creating Glue context
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Reading source data from source catalogue table - machinelearning.
# This table is created by crawler by reading the source data from s3 files.
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="reddit_csv_s3_catalogue",
    table_name="machinelearning",
    transformation_ctx="datasource0",
)
# applying mappings from source to target
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("id", "string", "id", "string"),
        ("created", "timestamp", "created", "timestamp"),
        ("url", "string", "url", "string"),
        ("selftext", "string", "selftext", "string"),
        ("upvote_ratio", "double", "upvote_ratio", "double"),
        ("author", "string", "author", "string"),
        ("author_premium", "boolean", "author_premium", "boolean"),
        ("over_18", "boolean", "over_18", "boolean"),
    ],
    transformation_ctx="applymapping1",
)

selectfields2 = SelectFields.apply(
    frame=applymapping1,
    paths=[
        "selftext",
        "upvote_ratio",
        "created",
        "author",
        "over_18",
        "id",
        "author_premium",
        "last_modified",
        "url",
    ],
    transformation_ctx="selectfields2",
)

resolvechoice3 = ResolveChoice.apply(
    frame=selectfields2,
    choice="MATCH_CATALOG",
    database="reddit_rds_mysql_catalogue",
    table_name="reddit_hot_posts",
    transformation_ctx="resolvechoice3",
)

resolvechoice4 = ResolveChoice.apply(
    frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4"
)

# Creating spark DFs from glue Dynamic frame read from source catalogue table above.
source_df = resolvechoice4.toDF().alias("source_df")

# reformatting selftext column to have its original data.
source_df = source_df.withColumn(
    "selftext", regexp_replace("selftext", "((comma))", ",")
)

# Reading Target data from target catalogue table - reddit_hot_posts.
# This table is created by crawler by reading the target data from MySQL DB.
targetdata = glueContext.create_dynamic_frame.from_catalog(
    database="reddit_rds_mysql_catalogue",
    table_name="reddit_hot_posts",
    transformation_ctx="targetdata",
)
# Creating spark DFs from glue Dynamic frame read from target catalogue table above.
target_df = targetdata.toDF().alias("target_df")

# performing left_anti join on above source and target spark dfs,
# to avoid insertion of duplicate records
left_join = source_df.join(target_df, "id", "left_anti")

# This will update the last_modified column with current run timestamp,
# for the new rows inserted.
# Added this column, as it will be useful during analysis.
left_join = left_join.withColumn("last_modified", F.current_timestamp())

# converting back the above spark df formed from left join to glue DF
final_DF = DynamicFrame.fromDF(left_join, glueContext, "nested")

# new data after left_anti join will now be loaded to target Db
datasink1 = glueContext.write_dynamic_frame.from_catalog(
    frame=final_DF,
    database="reddit_rds_mysql_catalogue",
    table_name="reddit_hot_posts",
    transformation_ctx="datasink1",
)
job.commit()
