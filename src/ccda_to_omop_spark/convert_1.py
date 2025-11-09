
import logging
import os
import pandas as pd

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from ccda_to_omop import layer_datasets as LD
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
dir="/Users/croeder/git/CCDA-data/resources"
filepath="/Users/croeder/git/CCDA-data/resources/CCDA_CCD_b1_InPatient_v2.xml"
filename="CCDA_CCD_b1_InPatient_v2.xml"


# this is a COPY of LD.process_dictory, only that one doesn't return the datasest!
def process_directory(directory_path, export_datasets, write_csv_flag):
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
   
    only_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    for file in (only_files):
        if file.endswith(".xml"):
            new_data_dict = LD.process_file(os.path.join(directory_path, file), write_csv_flag)
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
    return omop_dataset_dict

# Create pandas dataframes
##omop_dataset_dict = LD.process_file(os.path.join(dir, filepath), True)

print("1================")
if True:
    print(f"{dir}")
    for f in os.listdir(dir):
        print(f)
    only_files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
    for file in (only_files):
        print(f"{dir} {file}")
        if file.endswith(".xml"):
            print(f" would process {file}")
print("2================")
#omop_dataset_dict = LD.process_directory(dir, False, True)
omop_dataset_dict = process_directory(dir, False, True)
print("3================")

print(ddl.config_to_domain_name_dict)
for (cfg_name, dataset) in omop_dataset_dict.items():
    omop_domain = ddl.config_to_domain_name_dict[cfg_name]
    # debug type problems
    if omop_domain == 'Visit':
        print(f"SKIPPING {cfg_name} {omop_domain}")
    elif omop_domain == 'Observation':
        print(f"SKIPPING {cfg_name} {omop_domain}")
    else:
        print(f"DOING {cfg_name} {omop_domain}")
        if omop_domain in ddl.domain_name_to_table_name:
            omop_tablename = ddl.domain_name_to_table_name[omop_domain]
            sdf = None
            try:
                sdf = spark.createDataFrame(dataset, 
                    spark_dataframe_column_types.spark_dataframe_column_types[omop_tablename])
            except Exception as e:
                print(f"   ERROR converting parquet/{omop_domain}_parquet")
                print(f"   {e}")
            if sdf:
                try:
                    print(f"  WRITING parquet/{omop_domain}_parquet")
                    sdf.write.parquet(f"parquet/{omop_domain}_parquet", mode='overwrite')
                except Exception as e:
                    print(f"   ERROR writing parquet/{omop_domain}_parquet")
                    print(f"   {e}")
        else:
            print(f"   SKIPPED skipped \"{omop_domain}\" inot in domain_to_table_name") 

