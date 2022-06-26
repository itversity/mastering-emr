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

* Run the application after adding environment variables. Validate for multiple days.
  * 2021-01-13
  * 2021-01-14
  * 2021-01-15
* Here are the export statements to set the environment variables.

```shell script
export ENVIRON=PROD
export SRC_DIR=s3://aigithub/landing/ghactivity
export SRC_FILE_FORMAT=json
export TGT_DIR=s3://aigithub/emrraw/ghactivity
export TGT_FILE_FORMAT=parquet

export PYSPARK_PYTHON=python3
```

* Here are the spark submit commands to run application for 3 dates.

```shell script
export SRC_FILE_PATTERN=2021-01-13

spark-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py

export SRC_FILE_PATTERN=2021-01-14

spark-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py

export SRC_FILE_PATTERN=2021-01-15

spark-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py
```
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
tgt_df.groupBy('year', 'month', 'day').count().show()
```

