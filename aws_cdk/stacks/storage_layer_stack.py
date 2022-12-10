from aws_cdk import (
    Stack,
    aws_s3 as s3
)
from constructs import Construct
import aws_cdk as cdk
import os

class StorageLayer(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        raw_bucket = s3.Bucket(
            self, 
            'raw-bucket', 
            bucket_name=os.getenv('RAW_BUCKET')
        )

        stg_bucket = s3.Bucket(
            self, 
            'staging-bucket', 
            bucket_name=os.getenv('STAGING_BUCKET')
        )
