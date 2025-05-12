import sys
from awsglue.transforms import *  # ★ Already includes DropNullFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame  # ✅ Keep this

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

predicate_pushdown = "region in ('ca','gb','us')"

# Script generated for node Amazon S3
AmazonS3_node1746982837007 = glueContext.create_dynamic_frame.from_catalog(
    database="de_youtube_data",
    table_name="raw_statistics",
    transformation_ctx="AmazonS3_node1746982837007",
    push_down_predicate=predicate_pushdown
)

# Script generated for node Change Schema
ChangeSchema_node1746983054322 = ApplyMapping.apply(
    frame=AmazonS3_node1746982837007,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "string", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "string", "views", "bigint"),
        ("likes", "string", "likes", "bigint"),
        ("dislikes", "string", "dislikes", "bigint"),
        ("comment_count", "string", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "string", "comments_disabled", "boolean"),
        ("ratings_disabled", "string", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "string", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")  # ★ Ensure region is included
    ],
    transformation_ctx="ChangeSchema_node1746983054322"
)

# ★ FIX: Apply DropNullFields correctly
dropnullfields3 = DropNullFields.apply(frame=ChangeSchema_node1746983054322)

# ★ Convert to Spark DataFrame, coalesce and back to DynamicFrame
datasink1 = dropnullfields3.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Script generated for node Data Quality Evaluation
EvaluateDataQuality().process_rows(
    frame=ChangeSchema_node1746983054322,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1746982795711",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Script generated for node Amazon S3 output with partitioning ★
AmazonS3_node1746983224719 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,  # ★ Use the cleaned, coalesced frame
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data-youtube-ap-south-cleandata-lambda/youtube/raw_statistics/",
        "partitionKeys": ["region"]  # ★ Ensure partitioning is applied
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1746983224719"
)

job.commit()
