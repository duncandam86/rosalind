import os
from dotenv import load_dotenv
import logging
from typing import List, Union, Tuple
import numpy as np

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs, csv

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# load environment variables
load_dotenv()


def initiate_filesystem(fs_type):
    """
    Function to initialize S3 filesystem
    """
    if fs_type == "s3":
        sys = fs.S3FileSystem(
            access_key=os.environ["ACCESS_KEY"],
            secret_key=os.environ["SECRET_ACCESS_KEY"],
        )
    elif fs_type == "local":
        sys = fs.LocalFileSystem()

    return sys


def read_csv(
    path: str,
    bucket: str = os.environ["BUCKET"],
    compression: str = "detect",
    delimiter: str = ",",
    column_names: List[str] = [],
    skip_rows: int = 0,
    skip_rows_after_names: int = 0,
    fs_type="s3",
    read_mode="pyarrow",
):
    """
    Function to read csv file
    """

    filesystem = initiate_filesystem(fs_type)

    # read file

    with filesystem.open_input_stream(
        f"{bucket}/{path}", compression=compression
    ) as file:
        if read_mode == "pyarrow":
            table = csv.read_csv(
                file,
                parse_options=csv.ParseOptions(delimiter=delimiter),
                read_options=csv.ReadOptions(
                    column_names=column_names,
                    skip_rows=skip_rows,
                    skip_rows_after_names=skip_rows_after_names,
                ),
            )
        else:
            if len(column_names) == 0:
                df = pd.read_csv(file, sep=delimiter, skiprows=skip_rows)
            else:
                df = pd.read_csv(
                    file, sep=delimiter, skiprows=skip_rows, names=column_names
                )

            table = pa.table(df)

    return table


def read_parquet(
    path: str,
    filesystem: fs.FileSystem = initiate_filesystem("s3"),
    columns: List[str] = None,
    filters: Union[List[Tuple], List[List[Tuple]]] = None,
    use_threads: bool = True,
):
    """
    Function to read parquet table/file
    """
    bucket = os.environ["BUCKET"]

    # read parquet file
    table = pq.read_table(
        source=f"{bucket}/{path}",
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
    filesystem: fs.FileSystem = initiate_filesystem("s3"),
):
    """
    Function to write table to parquet file
    """
    bucket = os.environ["BUCKET"]

    # write table to s3 as parquet file
    pq.write_table(
        table=table,
        where=f"{bucket}/{path}",
        row_group_size=row_group_size,
        filesystem=filesystem,
    )


def explode(table: pa.Table, column: str) -> pa.Table:
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
