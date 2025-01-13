import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node Amazon S3
AmazonS3_node1736445105095 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ";", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://b3-data-bucket/bovespa/bovespa/IBOVDia_09-01-25.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1736445105095")

# Script generated for node Aggregate
Aggregate_node1736454262355 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1736445105095, groups = ["date", "setor", "acao"], aggs = [["qtde_teorica", "sum"]], transformation_ctx = "Aggregate_node1736454262355")

# Script generated for node Rename Field
RenameField_node1736789858726 = RenameField.apply(frame=Aggregate_node1736454262355, old_name="`sum(qtde_teorica)`", new_name="quantidade_teorica", transformation_ctx="RenameField_node1736789858726")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1736789858726, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736791022336", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1736791045334 = glueContext.getSink(path="s3://b3-data-bucket-destino/refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["date", "acao"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1736791045334")
AmazonS3_node1736791045334.setCatalogInfo(catalogDatabase="fiap_b3",catalogTableName="tb_b3_etl_fase2")
AmazonS3_node1736791045334.setFormat("glueparquet", compression="snappy")
AmazonS3_node1736791045334.writeFrame(RenameField_node1736789858726)
job.commit()