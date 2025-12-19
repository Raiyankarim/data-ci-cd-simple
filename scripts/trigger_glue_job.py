import boto3

glue = boto3.client("glue", region_name="us-east-1")

response = glue.start_job_run(
    JobName="orders-etl-job",
    Arguments={
        "--SOURCE_S3_PATH": "s3://de-cicd-raw-bucket/orders/",
        "--TARGET_S3_PATH": "s3://de-cicd-processed-bucket/orders/"
    }
)

print("Glue Job Triggered")
print("Run ID:", response["JobRunId"])