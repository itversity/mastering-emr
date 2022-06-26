# Getting Started

Here is how we can get started with development of data engineering pipelines using Spark on EMR Cluster.
* Connect to Master node of the EMR Cluster and create a folder by name **itv-ghactivity** under workspace **mastering-emr**.
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