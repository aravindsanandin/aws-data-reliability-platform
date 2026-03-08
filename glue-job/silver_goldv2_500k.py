import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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
AWSGlueDataCatalog_node1772900359435 = glueContext.create_dynamic_frame.from_catalog(database="sales_data_catalog", table_name="silver_cleaned_sales_v2", transformation_ctx="AWSGlueDataCatalog_node1772900359435")

# Script generated for node SQL Query
SqlQuery5156 = '''
SELECT
    country,
    SUM(revenue) AS total_revenue,
    COUNT(txn_id) AS total_transactions,
    AVG(revenue) AS avg_transaction_value
FROM myDataSource
GROUP BY country
'''
SQLQuery_node1772900436079 = sparkSqlQuery(glueContext, query = SqlQuery5156, mapping = {"myDataSource":AWSGlueDataCatalog_node1772900359435}, transformation_ctx = "SQLQuery_node1772900436079")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1772900436079, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772904051213", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1772904398727 = glueContext.getSink(path="s3://de-reliability-platform-jack/gold/sales_summary/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1772904398727")
AmazonS3_node1772904398727.setCatalogInfo(catalogDatabase="sales_data_catalog",catalogTableName="gold_sales_summary")
AmazonS3_node1772904398727.setFormat("glueparquet", compression="snappy")
AmazonS3_node1772904398727.writeFrame(SQLQuery_node1772900436079)
job.commit()