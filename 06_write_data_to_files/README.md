# Write data to files

Let us develop the code to write Spark Dataframe to the files using Spark Dataframe APIs.
* Create a Python program by name **write.py**. We will create a function by name **to_files**. It writes the data from Dataframe into files.

```python
def to_files(df, tgt_dir, file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'dayofmonth'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)
```

* Call the program from **app.py** to write Dataframe to files.

```python
import os
from util import get_spark_session
from read import from_files
from write import write
from process import transform


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Reading and Writing Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    write(df_transformed, tgt_dir, tgt_file_format)


if __name__ == '__main__':
    main()
```
* First cleanup all the files in the target location.
```
aws s3 rm s3://aigithub/emrraw/ghactivity \
    --recursive
```
* Run the application after adding environment variables. Validate for multiple days.
  * 2021-01-13
  * 2021-01-14
  * 2021-01-15
* You can use below command for the reference.

```
export ENVIRON=DEV
export SRC_DIR=s3://aigithub/landing/ghactivity
export SRC_FILE_PATTERN=2021-01-13
export SRC_FILE_FORMAT=json
export TGT_DIR=s3://aigithub/emrraw/ghactivity
export TGT_FILE_FORMAT=parquet

spark-submit \
    --master local \
    app.py
```
* Check for files in the target location. 

```shell script
aws s3 ls s3://aigithub/emrraw/ghactivity \
    --recursive
```

* Run below code using pyspark CLI.

```python
file_path = 's3://aigithub/raw/ghactivity'
df = spark.read.parquet(file_path)
df.printSchema()
df.show()
df. \
    groupBy('year', 'month', 'dayofmonth'). \
    count(). \
    show()
df.count()
```