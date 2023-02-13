import os
from dotenv import load_dotenv
import logging

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import fs, csv

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#load environment variables
load_dotenv()

def initiate_s3_filesystem():
    s3 = fs.S3FileSystem(
        access_key = os.environ["ACCESS_KEY"], 
        secret_key = os.environ["SECRET_ACCESS_KEY"]
    )

    return s3

def read(path:str, delimiter:str = ",", compression:str = "detect"):

    #initiate S3FileSystem
    s3 = initiate_s3_filesystem()

    #read file
    with s3.open_input_stream(path, compression=compression) as file:
        table = csv.read_csv(
            file, 
            parse_options=csv.ParseOptions(
                delimiter=delimiter
            )
        )
    
    return table

def explode(table, column):
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    
    indices = pc.list_parent_indices(table[column])
    result = table.select(other_columns).take(indices)
    result = result.append_column(
        pa.field(column, table.schema.field(column).type.value_type), 
        pc.list_flatten(table[column])
    )
    
    return result