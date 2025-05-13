YouTube ETL Pipeline using AWS Glue
This project demonstrates an ETL (Extract, Transform, Load) pipeline built using AWS Glue to process YouTube trending video data stored in Amazon S3.

📌 Project Overview
The pipeline performs the following steps:

1. Extracts raw CSV files from an S3 bucket (youtube/raw_statistics/)

2. Transforms the data (e.g., type conversion, schema mapping, joins with category reference data)

3. Cleans the data by applying basic data quality checks

4. Loads the final output as compressed Parquet files to another S3 bucket, partitioned by region and category_id

Additional processing is done using a Lambda function to handle JSON data alongside CSV files, integrating the data into the ETL flow.

🧰 Tools & Services Used
1. AWS S3 – Storage for raw and processed data

2. AWS Glue – ETL orchestration (crawler, job, dynamic frames, schema)

3. PySpark / Spark – For transformations and joins

4. AWS Lambda – Function to handle specific data (e.g., JSON)

5. AWS IAM – Permissions and job roles

🗂️ Directory Structure

youtube-etl-project/
├── README.md                     # Project documentation
├── etl_analytics_join.py         # Analytics join Glue job script
├── etl_script.py                 # AWS Glue job script
└── lambda_function.py            # Lambda function for processing JSON

🚀 Running the ETL Pipeline

1. Setup S3 Buckets:

   -> Create an S3 bucket for raw data: youtube/raw_statistics/

   -> Create another S3 bucket for processed data (Parquet output), partitioned by region and category_id.

2. Set Up AWS Glue Jobs:

   -> Create a Glue job for the extraction and transformation steps using etl_script.py.

   -> Create another Glue job for any additional analytics processing with etl_analytics_join.py.

3. Lambda Integration (if applicable):

   -> Deploy the lambda_function.py to AWS Lambda, which processes JSON data in pipeline.

4. Execute the Glue Jobs:

   -> Trigger the Glue jobs manually or schedule them via AWS Glue triggers.

5. Check Results:

   -> Processed data should appear in output S3 bucket, in Parquet format, partitioned by region and category_id.

📝 Notes

1. The project relies on AWS Glue and AWS Lambda to handle large-scale data processing.

2. Consider using AWS Glue Crawlers for automatic schema detection and cataloging.

3. You can visualize processed data with Amazon Quicksight for advanced analytics.
