
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
        level=logging.INFO
)
logger = logging.getLogger(__name__)

#filepath="/Users/croeder/git/CCDA-data/resources/CCDA_CCD_b1_Ambulatory_v2.xml" # has nulls in person birthdate
#filepath="/Users/croeder/git/CCDA-data/resources/170.314b2_AmbulatoryToC.xml"
#filepath="/Users/croeder/git/CCDA-data/resources/C-CDA_R2-1_CCD.xml"
#filepath="/Users/croeder/git/CCDA-data/resources/Patient-502.xml"
filepath="/Users/croeder/git/CCDA-data/resources/CCDA_CCD_b1_InPatient_v2.xml"

def process_file(directory_path):
    #new_data_dict = layer_datasets.process_file(os.path.join(directory_path, filepath), False, True)
    new_data_dict = layer_datasets.process_file(os.path.join(directory_path, filepath), True)
    for key in new_data_dict:
         if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
             if new_data_dict[key] is  not None:
                 omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
             else:
                 omop_dataset_dict[key]= new_data_dict[key]
         if new_data_dict[key] is not None:
             logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
         else:
              logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")

    return layer_datasets.combine_datasets(omop_dataset_dict)



omop_dataset_dict = process_file(filepath)

for (omop_domain, pdf) in omop_dataset_dict.items():
    # debug type problems
    if omop_domain == 'Visit':
        print(f"SKIPPING {omop_domain}")
    elif omop_domain == 'Observation':
        print(f"SKIPPING {omop_domain}")
    else:
        omop_tablename = ddl.domain_name_to_table_name[omop_domain]
        sdf = spark.createDataFrame(pdf, 
          spark_dataframe_column_types.spark_dataframe_column_types[omop_tablename])
        sdf.write.parquet(f"parquet/{omop_domain}_parquet", mode='overwrite')

