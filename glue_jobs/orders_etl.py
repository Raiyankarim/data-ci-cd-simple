import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'SOURCE_S3_PATH', 'TARGET_S3_PATH']
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

df = spark.read.option("header", "true") \
    .csv(args['SOURCE_S3_PATH'])

df_transformed = df.select(
    "order_id",
    "customer_id",
    "order_amount"
)

df_transformed.write.mode("overwrite") \
    .parquet(args['TARGET_S3_PATH'])

job.commit()