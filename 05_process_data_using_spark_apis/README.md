# Process data using Spark APIs

We will eventually partition the data by year, month and day of month while writing to target directory. However, to partition the data we need to add new columns.
* Create a Python program by name **process.py**. We will create a function by name **df_transform**. It partitions the Dataframe using specified field.

```python
from pyspark.sql.functions import year, \
    month, dayofmonth


def transform(df):
    return df.withColumn('year', year('created_at')). \
        withColumn('month', month('created_at')). \
        withColumn('dayofmonth', dayofmonth('created_at'))
```

* Call the program from **app.py**. For now review schema and data.

```python
import os
from util import get_spark_session
from read import from_files
from process import transform


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Partitioning Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    df_transformed.printSchema()
    df_transformed.select('repo.*', 'year', 'month', 'dayofmonth').show()


if __name__ == '__main__':
    main()
```
* Run the program to confirm that the changes are working as expected.

```
export ENVIRON=DEV
export SRC_DIR=s3://aigithub/landing/ghactivity
export SRC_FILE_PATTERN=2021-01-15
export SRC_FILE_FORMAT=json

spark-submit \
    --master local \
    app.py
```
