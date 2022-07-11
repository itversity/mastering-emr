# Create Function for SparkSession

Let us create a program and define function which returns spark session object. Make sure to update the code in **itv-ghactivity** folder.
* Define function by name **get_spark_session** in a program called as **util.py**

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
    return
```

* Refactor code in **app.py** to use the function to get Spark Session.

```python
import os
from util import get_spark_session


def main():
    env = os.environ.get('ENVIRON')
    spark = get_spark_session(env, 'GitHub Activity - Getting Started')
    spark.sql('SELECT current_date').show()


if __name__ == '__main__':
    main()
```
* Configure run time environment variable by name **ENVIRON** with value **DEV** to validate locally.
* Run the program to confirm that the changes are working as expected.

```
export ENVIRON=DEV
spark-submit \
    --master local \
    app.py
```