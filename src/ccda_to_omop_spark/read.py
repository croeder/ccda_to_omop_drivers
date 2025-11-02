
import logging
import os
import pandas as pd

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from ccda_to_omop import layer_datasets
from ccda_to_omop import spark_dataframe_column_types
from ccda_to_omop import ddl

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(
        format='%(levelname)s: %(filename)s %(lineno)d %(message)s',
#        filename=f"logs/log_file_{base_name}.log",
#        force=True,
         #level=logging.ERROR
         #level=logging.WARNING
         level=logging.INFO
        # level=logging.DEBUG
)
logger = logging.getLogger(__name__)


parquet_dirs = [ 
    "Care_Site_parquet",
    "Condition_parquet",
    "Device_parquet",
    "Drug_parquet",
    "Location_parquet",
    "Measurement_parquet",
    "Person_parquet",
    "Procedure_parquet",
    "Provider_parquet"
]
base="/Users/croeder/git/ccda_to_omop_simple_driver/src/ccda_to_omop_spark"
for pq_dir in parquet_dirs:
    print(pq_dir)
    df = spark.read.parquet(f"{base}/{pq_dir}")
    df.show(100)
