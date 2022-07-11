# Run using Cluster Mode

Let us go ahead and build zip file so that we can run the application in cluster mode.
* Here are the commands to build the zip file. We need to run these commands from **itv-ghactivity** folder in the workspace.
```
rm -f itv-ghactivity.zip
zip -r itv-ghactivity.zip *.py
```
* Copy the files into HDFS landing folders.

```shell script
aws s3 rm s3://aigithub/emrraw/ghactivity \
    --recursive

# Validating Files in HDFS
aws s3 ls s3://aigithub/landing/ghactivity \
    --recursive|grep json.gz|wc -l
```

* Run the application using Cluster Mode. Here the zip file and driver program file are in the local file system on the Master Node of the cluster.
```
spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf "spark.yarn.appMasterEnv.ENVIRON=PROD" \
	--conf "spark.yarn.appMasterEnv.SRC_DIR=s3://aigithub/landing/ghactivity" \
	--conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" \
	--conf "spark.yarn.appMasterEnv.TGT_DIR=s3://aigithub/emrraw/ghactivity" \
	--conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" \
	--conf "spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2021-01-13" \
	--py-files itv-ghactivity.zip \
	app.py
```
* Here are the instructions to run the application by using the zip file and the driver program from s3.
```
aws s3 cp itv-ghactivity.zip s3://aigithub/app/itv-ghactivity.zip
aws s3 cp app.py s3://aigithub/app/app.py

spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf "spark.yarn.appMasterEnv.ENVIRON=PROD" \
	--conf "spark.yarn.appMasterEnv.SRC_DIR=s3://aigithub/landing/ghactivity" \
	--conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" \
	--conf "spark.yarn.appMasterEnv.TGT_DIR=s3://aigithub/emrraw/ghactivity" \
	--conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" \
	--conf "spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2021-01-14" \
	--py-files s3://aigithub/app/itv-ghactivity.zip \
	s3://aigithub/app/app.py
```
* We can also run the application as step on existing EMR Cluster. We need to ensure both zip file as well as driver program file are in s3. You can use the previous command as reference. The command is supposed to be in single line discarding spark-submit, master and deploy-mode.
* Check for files in the target location. 

```shell script
aws s3 ls s3://aigithub/emrraw/ghactivity \
    --recursive
```

* We can use `pyspark2 --master yarn` to launch Pyspark and run the below code to validate.

```python
src_file_path = 's3://aigithub/landing/ghactivity'
src_df = spark.read.json(src_file_path)
src_df.printSchema()
src_df.show()
src_df.count()
from pyspark.sql.functions import to_date
src_df.groupBy(to_date('created_at').alias('created_at')).count().show()

tgt_file_path = f's3://aigithub/emrraw/ghactivity'
tgt_df = spark.read.parquet(tgt_file_path)
tgt_df.printSchema()
tgt_df.show()
tgt_df.count()
tgt_df.groupBy('year', 'month', 'dayofmonth').count().show()
```

