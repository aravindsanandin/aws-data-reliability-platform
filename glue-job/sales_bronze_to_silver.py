"""
AWS Glue ETL Job
Bronze → Silver Data Processing Pipeline

Responsibilities:
- Load raw CSV data from Bronze layer
- Perform schema mapping
- Standardize date formats
- Apply data quality rules
- Split valid and invalid records
- Remove duplicates
- Store cleaned data in Silver layer
- Send bad records to error quarantine layer
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1772881591963 = glueContext.create_dynamic_frame.from_catalog(database="sales_data_catalog", table_name="raw_source_sales_v2", transformation_ctx="AWSGlueDataCatalog_node1772881591963")

# Script generated for node Change Schema
ChangeSchema_node1772875364047 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1772881591963, mappings=[("txn_id", "long", "txn_id", "long"), ("customer", "string", "customer", "string"), ("revenue", "string", "revenue", "double"), ("date", "string", "date", "string"), ("country", "string", "country", "string")], transformation_ctx="ChangeSchema_node1772875364047")

# Script generated for node SQL Query
SqlQuery84 = '''
SELECT
txn_id,
customer,
revenue,
date_format(
    to_date(regexp_replace(date,'/','-'),'yyyy-MM-dd'),
    'yyyy/MM/dd'
) AS date,
country
FROM myDataSource
'''
SQLQuery_node1772895092471 = sparkSqlQuery(glueContext, query = SqlQuery84, mapping = {"myDataSource":ChangeSchema_node1772875364047}, transformation_ctx = "SQLQuery_node1772895092471")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1772882215321_ruleset = """
    Rules = [
    ColumnExists "txn_id",
    ColumnExists "customer",
    ColumnExists "revenue",
    ColumnExists "date",
    ColumnExists "country",
    IsComplete "txn_id",
    IsComplete "customer",
    IsComplete "country",
    ColumnValues "revenue" > 0
    ]

"""

EvaluateDataQuality_node1772882215321 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1772895092471, ruleset=EvaluateDataQuality_node1772882215321_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772882215321", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1772882681758 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1772882215321, key="originalData", transformation_ctx="originalData_node1772882681758")

# Script generated for node ruleOutcomes
ruleOutcomes_node1772883172687 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1772882215321, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1772883172687")

# Script generated for node invalid_records_filter
invalid_records_filter_node1772884467429 = Filter.apply(frame=originalData_node1772882681758, f=lambda row: (row["revenue"] <= 0), transformation_ctx="invalid_records_filter_node1772884467429")

# Script generated for node VALID_Filter
VALID_Filter_node1772876051537 = Filter.apply(frame=originalData_node1772882681758, f=lambda row: (row["revenue"] > 0), transformation_ctx="VALID_Filter_node1772876051537")

# Script generated for node invalid_SQL
SqlQuery85 = '''
SELECT
    txn_id,
    date,
    country,
    revenue,
    customer
FROM myDataSource
WHERE
    txn_id IS NULL
    OR revenue IS NULL
    OR revenue <= 0
    OR date IS NULL
    OR TRIM(date) = ''
    OR country IS NULL
    OR TRIM(country) = ''
    OR customer IS NULL
    OR TRIM(customer) = ''

    -- invalid date format
    OR NOT date RLIKE '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'

    -- future dates
    OR to_date(regexp_replace(date,'/','-'),'yyyy-MM-dd') > current_date()
'''
invalid_SQL_node1772897713882 = sparkSqlQuery(glueContext, query = SqlQuery85, mapping = {"myDataSource":invalid_records_filter_node1772884467429}, transformation_ctx = "invalid_SQL_node1772897713882")

# Script generated for node valid_SQL
SqlQuery83 = '''
SELECT
    txn_id,
    date_format(
        to_date(regexp_replace(date,'/','-'),'yyyy-MM-dd'),
        'yyyy/MM/dd'
    ) AS date,
    UPPER(country) AS country,
    revenue,
    customer
FROM myDataSource
WHERE
    txn_id IS NOT NULL
    AND revenue IS NOT NULL
    AND revenue > 0
    AND date IS NOT NULL
    AND TRIM(date) <> ''
    AND country IS NOT NULL
    AND TRIM(country) <> ''
    AND customer IS NOT NULL
    AND TRIM(customer) <> ''

    -- enforce date format
    AND date RLIKE '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'

    -- remove future dates
    AND to_date(regexp_replace(date,'/','-'),'yyyy-MM-dd') <= current_date()
'''
valid_SQL_node1772897033866 = sparkSqlQuery(glueContext, query = SqlQuery83, mapping = {"myDataSource":VALID_Filter_node1772876051537}, transformation_ctx = "valid_SQL_node1772897033866")

# Script generated for node Drop Duplicates
DropDuplicates_node1772875228612 =  DynamicFrame.fromDF(valid_SQL_node1772897033866.toDF().dropDuplicates(["txn_id"]), glueContext, "DropDuplicates_node1772875228612")

# Script generated for node Invalid_buckket 
EvaluateDataQuality().process_rows(frame=invalid_SQL_node1772897713882, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772882158640", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Invalid_buckket_node1772884999643 = glueContext.getSink(path="s3://de-reliability-platform-jack/errors/bad_sales_records/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Invalid_buckket_node1772884999643")
Invalid_buckket_node1772884999643.setCatalogInfo(catalogDatabase="sales_data_catalog",catalogTableName="error_bad_sales_records")
Invalid_buckket_node1772884999643.setFormat("glueparquet", compression="snappy")
Invalid_buckket_node1772884999643.writeFrame(invalid_SQL_node1772897713882)
# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1772875228612, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772874888540", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1772876256628 = glueContext.getSink(path="s3://de-reliability-platform-jack/silver/cleaned_sales/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1772876256628")
AmazonS3_node1772876256628.setCatalogInfo(catalogDatabase="sales_data_catalog",catalogTableName="silver_cleaned_sales_v2")
AmazonS3_node1772876256628.setFormat("glueparquet", compression="snappy")
AmazonS3_node1772876256628.writeFrame(DropDuplicates_node1772875228612)
job.commit()