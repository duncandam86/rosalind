import os
import re
from functools import wraps
from typing import Dict, List, Tuple, Union, Callable, Any
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import findspark
findspark.init()

from pyspark.sql import SparkSession


def get_spark_session(include_graphframes: bool = False, save_to_s3: bool = False):
    """
    Initializes spark session. Should only be required when not running on Databricks.

    Args:
        include_graphframes (bool): Flag to include graphframes package in
            ``spark.jars.packages``.

    Returns:
        SparkSession: Spark session.
    """

    builder = SparkSession.builder.config(
        "spark.driver.maxResultSize", "4g"
    )

    if include_graphframes:
        builder = builder.config(
            "spark.jars.packages", "graphframes:graphframes:0.8.0-spark3.0-s_2.12"
        )


    spark = builder.getOrCreate()

    return spark


def read_file(path, type, header = True):
    spark = get_spark_session()
    if type == "csv":
        df = spark.read.csv(path, header = header)
    elif type == "tsv":
        df = spark.read.csv(path, sep = "\t", header =  header)
    elif type == "parquet":
        df = spark.read.csv(path, header = header)
    
    return df

    
