# Getting Started

Here is how we can get started with development of data engineering pipelines using Spark on EMR Cluster.
* Open the project as Visual Studio Workspace using Remote Connection.
* Make sure to install required extensions such as Python/Pylance.
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

* Make sure to update **.env** file with PYTHONPATH pointing to Spark related modules already available on the master node of the cluster.

```
PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9.2-src.zip
```

* Try running the application using `spark-submit`. Make sure to run it from **itv-ghactivity** folder.

```
spark-submit \
    --master local \
    app.py
```