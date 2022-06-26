# Productionize Code

Let us make necessary changes to the code so that we can run on a multinode cluster.
* Update **util.py** to use multi node cluster.

```python
from pyspark.sql import SparkSession


def get_spark_session(env, app_name):
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark
    return
```
* Here are the commands to download the files.

```shell script
mkdir -p data/itv-github/landing/ghactivity
cd data/itv-github/landing/ghactivity

wget https://data.gharchive.org/2021-01-13-{0..23}.json.gz
wget https://data.gharchive.org/2021-01-14-{0..23}.json.gz
wget https://data.gharchive.org/2021-01-15-{0..23}.json.gz
```

* Copy the files into HDFS landing folders.

```shell script
hdfs dfs -mkdir -p /user/${USER}/itv-github/landing/ghactivity
hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity
hdfs dfs -put * /user/${USER}/itv-github/landing/ghactivity/

# Validating Files in HDFS
hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity
hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity|wc -l
hdfs dfs -du -s -h /user/${USER}/itv-github/landing/ghactivity
```

* Run the application after adding environment variables. Validate for multiple days.
  * 2021-01-13
  * 2021-01-14
  * 2021-01-15
* Here are the export statements to set the environment variables.

```shell script
export ENVIRON=PROD
export SRC_DIR=/user/${USER}/itv-github/landing/ghactivity
export SRC_FILE_FORMAT=json
export TGT_DIR=/user/${USER}/itv-github/raw/ghactivity
export TGT_FILE_FORMAT=parquet

export PYSPARK_PYTHON=python3
```

* Here are the spark submit commands to run application for 3 dates.

```shell script
export SRC_FILE_PATTERN=2021-01-13

spark2-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py

export SRC_FILE_PATTERN=2021-01-14

spark2-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py

export SRC_FILE_PATTERN=2021-01-15

spark2-submit --master yarn \
    --py-files itv-ghactivity.zip \
    app.py
```
* Check for files in the target location. 

```shell script
hdfs dfs -find /user/${USER}/itv-github/raw/ghactivity
```

* We can use `pyspark2 --master yarn --conf spark.ui.port=0` to launch Pyspark and run the below code to validate.

```python
import getpass
username = getpass.getuser()

src_file_path = f'/user/{username}/itv-github/landing/ghactivity'
src_df = spark.read.json(src_file_path)
src_df.printSchema()
src_df.show()
src_df.count()
from pyspark.sql.functions import to_date
src_df.groupBy(to_date('created_at').alias('created_at')).count().show()

tgt_file_path = f'/user/{username}/itv-github/raw/ghactivity'
tgt_df = spark.read.parquet(tgt_file_path)
tgt_df.printSchema()
tgt_df.show()
tgt_df.count()
tgt_df.groupBy('year', 'month', 'day').count().show()
```

