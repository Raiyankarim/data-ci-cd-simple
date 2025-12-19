import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'SOURCE_S3_PATH', 'TARGET_S3_PATH']
)

print("SOURCE_S3_PATH:", args['SOURCE_S3_PATH'])
print("TARGET_S3_PATH:", args['TARGET_S3_PATH'])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args['JOB_NAME'], args)

print("Reading input CSV")

df = spark.read \
    .option("header", "true") \
    .csv(args['SOURCE_S3_PATH'])

print("Schema:")
df.printSchema()

record_count = df.count()
print("Record count:", record_count)

if record_count == 0:
    raise Exception("Input data is empty. Failing job.")

df_transformed = df.select(
    "order_id",
    "customer_id",
    "order_amount"
)

print("Writing Parquet output")

df_transformed \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(args['TARGET_S3_PATH'])

print("Write completed successfully")

job.commit()
