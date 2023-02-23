import sqlite3
import pandas.io.sql as sqlio
import pyarrow as pa

from ..utils import utils
from ..utils import log_utils


def sqlite_conn(path):
    conn = sqlite3.connect(path)

    return conn


def get_disgenet_tables(
    disgenet_path: str,
):
    # connect
    conn = sqlite_conn(disgenet_path)
    # get all tables in database
    df_disgenet = sqlio.read_sql_query(
        "SELECT * FROM sqlite_schema WHERE type='table'", conn
    )
    # get list of all tables
    disgenet_tables = df_disgenet["name"].values.tolist()

    return disgenet_tables


def write_table(table: str, path: str, conn):
    # load table from database
    df_table = sqlio.read_sql_query(f"SELECT * FROM {table}", conn)

    df_table = df_table.replace("NA", None, regex=True)

    try:
        # convert to pyarrow table
        table = pa.table(df_table)
        # write to parquet
        utils.write_parquet(table, f"{path}.parquet")
    except:
        s3 = utils.initiate_s3_filesystem()
        with s3.open_output_stream(f"rosalind-pipeline/{path}.tsv") as file:
            df_table.to_csv(file, index=None, sep="\t")


def run():
    disgenet_path = "/Users/duncandam/Documents/rosalind/resources/downloads/disgenet/disgenet_2020.db"

    # initiate connection
    conn = sqlite_conn(disgenet_path)
    # get all tables
    disgenet_tables = get_disgenet_tables(disgenet_path)
    log_utils.logger.info(f"Tables: {disgenet_tables}")

    for table in disgenet_tables:
        log_utils.logger.info(f"Writing out {table}")
        path = f"stage-0/{table}"
        write_table(table, path, conn)
        log_utils.logger.info(f"Finished writing out {table}")
