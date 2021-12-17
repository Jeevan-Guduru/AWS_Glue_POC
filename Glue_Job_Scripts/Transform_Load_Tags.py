#importing glue modules
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
#importing pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
#importing python modules
import pandas as pd
import sys


"""
Description:
This script is for transform and loading the tags into posts_treatment_tags table.
Main transformation applied here is to convert the list of tags into individual rows.

Tags are mapped to their corresponding posts id here.

"""

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# reading data from source catalogue
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="reddit_csv_s3_catalogue",
    table_name="machinelearning",
    transformation_ctx="datasource0",
)

#coverting Glue DF to Spark DF
source_df = datasource0.toDF().alias("source_df")

# selecting required fields from the source
temp_df = source_df.select("id", "treatment_tags")

# During extract and loading to CSV, all columns are changed to Strings by default.
# Below, converting those strings back to their original types,
# by changing the df to pandas and then converting to pyspark df by applying schema required.
temp_df = temp_df.toPandas()
temp_df["treatment_tags"] = pd.eval(temp_df["treatment_tags"])


schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("treatment_tags", ArrayType(StringType(), True), True),
    ]
)
temp_df = spark.createDataFrame(temp_df, schema=schema)

# exploding the list column to store individual elements as strings of separate rows.
# explode_outer is used to retain records with NULL values in the list
temp_df = temp_df.select(
    col("id"), explode_outer(col("treatment_tags")).alias("treatment_tags")
).select(col("id"), col("treatment_tags"))
targetdata = glueContext.create_dynamic_frame.from_catalog(
    database="reddit_rds_mysql_catalogue",
    table_name="reddit_posts_treatment_tags",
    transformation_ctx="targetdata",
)
target_df = targetdata.toDF().alias("target_df")

# Removing duplicates using left_anti join.
left_join = temp_df.join(target_df, "id", "left_anti")

# Spark DF is converted back to Glue Dynamic frame.
final_DF = DynamicFrame.fromDF(left_join, glueContext, "nested")
# Loading above dynamic frame to target Db.
datasink = glueContext.write_dynamic_frame.from_catalog(
    frame=final_DF,
    database="reddit_rds_mysql_catalogue",
    table_name="reddit_posts_treatment_tags",
    transformation_ctx="datasink",
)

job.commit()
