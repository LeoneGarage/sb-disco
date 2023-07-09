# Databricks notebook source
# MAGIC %sh
# MAGIC
# MAGIC echo $DATABRICKS_TOKEN

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cat > ~/.databrickscfg << EOF
# MAGIC [DEFAULT]
# MAGIC host = https://e2-demo-field-eng.cloud.databricks.com
# MAGIC token = $DATABRICKS_TOKEN
# MAGIC EOF

# COMMAND ----------

import submit

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC CLUSTER_SPEC='{ "cluster_name":"", "spark_version":"13.2.x-scala2.12", "instance_pool_id":"0703-011504-era331-pool-7mt36cl2", "driver_instance_pool_id":"0703-011504-era331-pool-7mt36cl2", "data_security_mode": "NONE", "runtime_engine":"STANDARD", "autoscale": { "min_workers":2, "max_workers":8 } }'
# MAGIC
# MAGIC # CLUSTER_SPEC='{ "cluster_name":"", "spark_version":"13.2.x-scala2.12", "node_type_id":"i3.xlarge", "driver_node_type_id":"m5.xlarge", "enable_elastic_disk":false, "data_security_mode":"NONE", "runtime_engine":"STANDARD", "autoscale": { "min_workers":2, "max_workers":8 } }'
# MAGIC
# MAGIC # export DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com
# MAGIC # export DATABRICKS_TOKEN=<PAT Token>
# MAGIC mkdir -p /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit; cp -r /dbfs/Users/leon.eller@databricks.com/zips/db_submit.zip /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/db_submit.zip;unzip -o /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/db_submit.zip -d /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit
# MAGIC /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/bin/db-submit --source-zip dbfs:/Users/leon.eller@databricks.com/zips/my_app.zip --dest-zip /home/hadoop/app/bronze/bronze_bwm.dummy_two --cluster-spec "$CLUSTER_SPEC" --conf spark.app.name=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 --conf spark.yarn.stagingDir=hdfs://$(hostname -f):8020/user/hadoop --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark --conf spark.yarn.submit.waitAppCompletion=true --conf spark.port.maxRetries=1000 --conf spark.yarn.tags=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 --conf spark.executor.asyncEagerFileSystemInit.paths=s3://datalake-python-repo-ap-southeast-2-dv2t4qi6u0ds --conf spark.kryoserializer.buffer.max=1024m --conf spark.default.parallelism=512 --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=512 --conf spark.dynamicAllocation.maxExecutors=6 --py-files /home/hadoop/app/bronze/bronze_bwm.dummy_two/my_app.zip --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.spark:spark-avro_2.12:3.3.2,com.fasterxml.jackson.core:jackson-databind:2.12.0 file:///home/hadoop/app/bronze/bronze_bwm.dummy_two/my_app/app/main.py --job demo_app --job-type bronze --job-args region_name=ap-southeast-2 pipeline_name=bronze_bwm.dummy_two pipeline_run_id=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 kafka_topic=dummy_two load_type=INCR dependencies=bronze_bwm.dummy_one bronze_bwm.dummy_one.batchId=1688383197 bronze_bwm.dummy_one.delta_table_path=s3://dl-bronze-bwm-pii-prd-lsmjik7uyv8/bronze_bwm/dummy_one bronze_bwm.dummy_one.None=None

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC CLUSTER_SPEC='{ "cluster_name":"", "spark_version":"13.2.x-scala2.12", "instance_pool_id":"0703-011504-era331-pool-7mt36cl2", "driver_instance_pool_id":"0703-011504-era331-pool-7mt36cl2", "data_security_mode": "NONE", "runtime_engine":"STANDARD", "autoscale": { "min_workers":2, "max_workers":8 } }'
# MAGIC
# MAGIC # export DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com
# MAGIC # export DATABRICKS_TOKEN=<PAT Token>
# MAGIC mkdir -p /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit; aws s3 cp s3://datalake-shared-resources-etl-artifact-prd-twycaj6ki1az/app/db_submit.zip /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/db_submit.zip;unzip -o /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/db_submit.zip -d /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit
# MAGIC /home/hadoop/app/bronze/bronze_bwm.dummy_two/db_submit/bin/db-submit --source-zip s3://datalake-shared-resources-etl-artifact-prd-twycaj6ki1az/app/bronze/demo_app/017390273/pipeline-package.zip --dest-zip /home/hadoop/app/bronze/bronze_bwm.dummy_two --cluster-spec "$CLUSTER_SPEC" --conf spark.app.name=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 --conf spark.yarn.stagingDir=hdfs://$(hostname -f):8020/user/hadoop --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark --conf spark.yarn.submit.waitAppCompletion=true --conf spark.port.maxRetries=1000 --conf spark.yarn.tags=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 --conf spark.executor.asyncEagerFileSystemInit.paths=s3://datalake-python-repo-ap-southeast-2-dv2t4qi6u0ds --conf spark.kryoserializer.buffer.max=1024m --conf spark.default.parallelism=512 --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=512 --conf spark.dynamicAllocation.maxExecutors=6 --py-files /home/hadoop/app/bronze/bronze_bwm.dummy_two/pipeline-package.zip,s3a://datalake-python-repo-ap-southeast-2-dv2t4qi6u0ds/packages/sportsbet/lakehouselib/pip/lakehouselib-latest.zip --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.spark:spark-avro_2.12:3.3.2,com.fasterxml.jackson.core:jackson-databind:2.12.0 file:///Workspace/Repos/leon.eller@databricks.com/sb-disco/main.py --job demo_app --job-type bronze --job-args region_name=ap-southeast-2 pipeline_name=bronze_bwm.dummy_two pipeline_run_id=bronze_bwm.dummy_two-2023-07-07T04:38:10.717 kafka_topic=dummy_two load_type=INCR dependencies=bronze_bwm.dummy_one bronze_bwm.dummy_one.batchId=1688383197 bronze_bwm.dummy_one.delta_table_path=s3://dl-bronze-bwm-pii-prd-lsmjik7uyv8/bronze_bwm/dummy_one bronze_bwm.dummy_one.None=None

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC aws s3
