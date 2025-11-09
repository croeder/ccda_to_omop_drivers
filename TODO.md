
## 2025-11-09
- Running in Spark, though not the cluster seems to run
- As before, Need to see how to submit basic PySpark over the cluster.
- Need to make a dataset of the files' strings so we can do the next step
- Move to driving executing through flatMap from such a dataset by adapting this to ccda_to_omop. Start from the CCDA_OMOP_Spark_Transform's omop_eav_dict.py


## 2025-11-02
  - We know how to submit Calculate Pi over the cluster. It's Java code.
  - Need to see how to submit basic PySpark over the cluster.
  - Then adapt this to ccda_to_omop. Start from the CCDA_OMOP_Spark_Transform's omop_eav_dict.py

## 2025-10-25
  - submit demo Java  program
   - start main:
   - start workers on each machine with main's IP: sudo /opt/spark/current/sbin/start-worker.sh 10.0.1.175:7077 -c 3 -m 4g
   - submit job: sudo  bin/spark-submit --master spark://10.0.1.175:7077  --deploy-mode client  --class org.apache.spark.examples.SparkPi /opt/spark/current/examples/jars/spark-examples_2.13-4.0.1.jar 10000
