
import datetime
import logging
import os
import pandas as pd
import pathlib

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, FloatType, DoubleType,
    StringType, DateType, TimestampType
)
from ccda_to_omop import layer_datasets as LD
from ccda_to_omop import value_transformations as VT
from ccda_to_omop import domain_dataframe_column_types
from ccda_to_omop import ddl
from ccda_to_omop.util import create_codemap_dict_from_csv
from process_directory import process_directory
from type_util import  coerce_dataframe
from convert_to_parquet import convert_to_parquet

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(
        format='%(levelname)s: %(filename)s %(lineno)d %(message)s',
        level=logging.INFO
)
logger = logging.getLogger(__name__)

#dir="/Users/croeder/git/CCDA-data/resources"

dir="/Users/croeder/git/CCDA/tislab-clad/CCDA_OMOP_Conversion_Package/resources"

home=pathlib.Path(__file__).parent.parent.parent.resolve()
codemap_dict = create_codemap_dict_from_csv(f"{home}/resources/map.csv")
VT.set_codemap_dict(codemap_dict)

omop_dataset_dict = process_directory(dir, False, '')
convert_to_parquet(omop_dataset_dict)


