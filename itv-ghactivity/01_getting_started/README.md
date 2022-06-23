# Getting Started

Here is how we can get started with local development of data engineering pipelines using Spark.
* Create Virtual Environment - `python3.7 -m venv itvg-venv`
* Activate virtual environment - `source itvg-venv/bin/activate`
* Install PySpark for local development - `pip install pyspark==2.4.*`
* Open using PyCharm and make sure appropriate virtual environment is used from the virtual environment which we have setup.
* Create a program called as **app.py** and enter this code.

```python
from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('GitHub Activity - Getting Started'). \
    getOrCreate()

spark.sql('SELECT current_date').show()
```