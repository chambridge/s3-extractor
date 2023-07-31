import os
import boto3
import csv
import json

s3 = boto3.resource('s3')

bucket_name = os.getenv("S3_BUCKET")
bucket = s3.Bucket(bucket_name)

customer_number = os.getenv("CUSTOMER")
source_type = os.getenv("SOURCE_TYPE")
source = os.getenv("SOURCE")
year = os.getenv("YEAR")
month = os.getenv("MONTH")
filter_keys_str = os.getenv("FILTER_KEYS",[])
filter_keys = json.loads(filter_keys_str)
prefix = f"data/csv/{customer_number}/{source_type}/source={source}/year={year}/month={month}/"

for bucket_object in bucket.objects.filter(Prefix=prefix):
    download = False
    if filter_keys:
        for filter_key in filter_keys:
            if filter_key in bucket_object.key:
                download = True
                break
    else:
        download = True
    if download:
        os.makedirs(os.path.dirname(bucket_object.key), exist_ok=True)
        bucket.download_file(bucket_object.key, bucket_object.key)
