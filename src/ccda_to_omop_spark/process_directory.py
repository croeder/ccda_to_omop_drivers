
from ccda_to_omop import layer_datasets as LD
import os
import pandas as pd
import logging
logger = logging.getLogger(__name__)

import datetime
import pathlib

from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, FloatType, DoubleType,
    StringType, DateType, TimestampType
)


# LD.process_directory() returns None (designed for Foundry), so this local
# version returns the accumulated dict instead.
# Also writes per-file per-config CSVs to output/ for comparison.
def process_directory(directory_path, write_csv_flag, parse_config, csv_output_dir="output"):
    omop_dataset_dict = {}  # keyed by config names
    os.makedirs(csv_output_dir, exist_ok=True)

    only_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    for file in only_files:
        if file.endswith(".xml"):
            new_data_dict = LD.process_file(os.path.join(directory_path, file), write_csv_flag, parse_config)
            for key in new_data_dict:
                if new_data_dict[key] is not None:
                    csv_path = os.path.join(csv_output_dir, f"{file}__{key}.csv")
                    new_data_dict[key].to_csv(csv_path, index=False, lineterminator='\r\n')
                if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                    if new_data_dict[key] is not None:
                        omop_dataset_dict[key] = pd.concat([omop_dataset_dict[key], new_data_dict[key]])
                else:
                    omop_dataset_dict[key] = new_data_dict[key]
                if new_data_dict[key] is not None:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                else:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")
    return omop_dataset_dict


