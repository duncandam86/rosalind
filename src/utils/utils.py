import pyarrow as pa
import pandas as pd
import boto3
import io
import os
from dotenv import load_dotenv
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#load environment variables
load_dotenv()

def create_client():
    """functions to create S3 client using AWS creds"""

    client = boto3.client(
        's3',
        aws_access_key_id = os.environ["ACCESS_KEY"],
        aws_secret_access_key = os.environ["SECRET_ACCESS_KEY"]
    )

    return client

def read_csv(key:str, compression:str=None, sep:str=","):

    #initiate clients
    s3 = create_client()

    #get object from s3 buckets
    obj = s3.get_object(
        Bucket=os.environ["BUCKET"],
        Key=key
    )

    if compression is not None:
        df = pd.read_csv(obj["Body"], compression=compression, sep=sep)
    else:
        df = pd.read_csv(obj["Body"], sep=sep)
    
    table = pa.Table.from_pandas(df)

    return table


