import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746997628612 = glueContext.create_dynamic_frame.from_catalog(database="de_youtube_data", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1746997628612", push_down_predicate=predicate_pushdown)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746997649362 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleansed", table_name="cleansed_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1746997649362")

# Script generated for node Join
Join_node1746997680153 = Join.apply(frame1=AWSGlueDataCatalog_node1746997628612, frame2=AWSGlueDataCatalog_node1746997649362, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1746997680153")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1746997680153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746997623297", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746997795129 = glueContext.getSink(path="s3://data-youtube-ap-south-analytics", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746997795129")
AmazonS3_node1746997795129.setCatalogInfo(catalogDatabase="db_yt_analysis",catalogTableName="final-table")
AmazonS3_node1746997795129.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746997795129.writeFrame(Join_node1746997680153)
job.commit()
