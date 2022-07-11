# Read data from files

Let us develop the code to read the data from files into Spark Dataframes.
* Create directory for data under **mastering-emr** and copy some files into it.

```shell script
mkdir -p data/itv-github/landing/ghactivity
cd data/itv-github/landing/ghactivity
wget https://data.gharchive.org/2021-01-13-0.json.gz
wget https://data.gharchive.org/2021-01-14-0.json.gz
wget https://data.gharchive.org/2021-01-15-0.json.gz
```
* Make sure to upload all the files to s3.
```
aws s3 rm s3://aigithub/landing/ghactivity/ \
    --recursive

aws s3 cp ~/mastering-emr/data/itv-github/landing/ghactivity \
    s3://aigithub/landing/ghactivity \
    --recursive
```
* Create a Python program by name **read.py**. We will create a function by name **from_files**. It reads the data from files into Dataframe and returns it.

```python
def from_files(spark, data_dir, file_pattern, file_format):
    df = spark. \
        read. \
        format(file_format). \
        load(f'{data_dir}/{file_pattern}')
    return df
```

* Call the program from **app.py**. For now review schema and data.

```python
import os
from util import get_spark_session
from read import from_files


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df.printSchema()
    df.select('repo.*').show()


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