import os
from dotenv import load_dotenv
import logging
from typing import List, Union, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs, csv


logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# load environment variables
load_dotenv()


def initiate_s3_filesystem():
    """
    Function to initialize S3 filesystem
    """
    s3 = fs.S3FileSystem(
        access_key=os.environ["ACCESS_KEY"], secret_key=os.environ["SECRET_ACCESS_KEY"]
    )

    return s3


def read_csv(path: str, delimiter: str = ",", compression: str = "detect"):
    """
    Function to read csv file
    """
    # initiate S3FileSystem
    s3 = initiate_s3_filesystem()

    # read file
    with s3.open_input_stream(path, compression=compression) as file:
        table = csv.read_csv(file, parse_options=csv.ParseOptions(delimiter=delimiter))

    return table


def read_parquet(
    path: str,
    file_system: fs.FileSystem,
    columns: List[str] = None,
    filters: Union[List[Tuple], List[List[Tuple]]] = None,
    use_threads: bool = True,
):
    """
    Function to read parquet table/file
    """
    # read parquet file
    table = pq.read_table(
        source=path,
        filesystem=filesystem,
        columns=columns,
        filters=filters,
        use_threads=use_threads,
    )

    return table


def write_parquet(
    table: pa.table,
    path: str,
    row_group_size: int = 1000000,
    filesystem: fs.FileSystem = initiate_s3_filesystem(),
):
    """
    Function to write table to parquet file
    """
    # write table to s3 as parquet file
    pq.write_table(
        table=table, where=path, row_group_size=row_group_size, filesystem=filesystem
    )


def explode(table, column):
    """
    Function to explode an array table to multiple record
    """
    other_columns = list(table.schema.names)
    other_columns.remove(column)

    indices = pc.list_parent_indices(table[column])
    result = table.select(other_columns).take(indices)
    result = result.append_column(
        pa.field(column, table.schema.field(column).type.value_type),
        pc.list_flatten(table[column]),
    )

    return result
