# AWS EMR Redshift Integration

Here are the steps need to be followed to validate AWS EMR and Redshift Integration.
* Add bootstrap script to s3
* Setup AWS EMR Cluster with Bootstrap script
* Create Elastic Ip for Redshift Cluster
* Setup Redshift Cluster
    * Make sure to choose non production
    * Enable Public Accessibility
    * Use Elastic Ip Created to point to Redshift Cluster
* Setup Database and User in Redshift Cluster
```
CREATE DATABASE github_dm;

CREATE USER github_user WITH PASSWORD 'G1tHub!23';

GRANT ALL ON DATABASE github_dm TO github_user;
```
* Create required tables in Redshift. Use github_user to connect to github_dm database on Redshift Cluster to create the table.
```
CREATE TABLE public.ghrepos (
  repo_id BIGINT DISTKEY,
  repo_name VARCHAR,
  actor_id BIGINT,
  actor_login VARCHAR,
  actor_display_login VARCHAR,
  ref_type VARCHAR,
  type VARCHAR,
  created_at VARCHAR,
  year INT,
  month INT,
  day INT
);
```
* Setup boto3 on the master node of AWS EMR Cluster using pip
* Create Secret for Redshift using AWS Secrets Manager
* Make sure to update permissions on the role **EMR_EC2_DefaultRole** to read the value for the secret **demo/github/redshift**. Make sure to specify the Secret Id from ARN.
```
import boto3
sm_client = boto3.client('secretsmanager')
secret_value = sm_client.get_secret_value(SecretId='demo/github/redshift')
secret_value['SecretString']
```
* Validate Redshift Connectivity from AWS EMR
    * Check security group inbound rule for Redshift Cluster.
    * Make sure to add rules for security groups associated with AWS EMR Masters and Slaves.
    * Use telnet to confirm the network connectivity from AWS EMR to Redshift.
* Optionally, we can also install psycopg2-binary and validate connectivity to Redshift from AWS EMR using Python.
```
import boto3
import json
sm_client = boto3.client('secretsmanager')
secret_value = sm_client.get_secret_value(SecretId='demo/github/redshift')
credentials = json.loads(secret_value['SecretString'])
import psycopg2
conn = psycopg2.connect(
    host=credentials['host'],
    port=credentials['port'],
    database='github_dm',
    user=credentials['username'],
    password=credentials['password']
)

query = '''
    SELECT * FROM information_schema.tables
    WHERE table_schema = 'public'
'''
cur = conn.cursor()
cur.execute(query)
for rec in cur.fetchall():
    print(rec)
```
* Launch Pyspark with required dependencies
```
pyspark \
  --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar
```
* Develop required logic using Pyspark to get only the new repositories added.

```
ghactivity = spark.read.json('s3://aigithub/landing/ghactivity/2022-06-19*')
ghactivity.createOrReplaceTempView('ghactivity')
new_repos = spark.sql("""
  SELECT
    repo.id AS repo_id,
    repo.name AS repo_name,
    actor.id AS actor_id,
    actor.login AS actor_login,
    actor.display_login AS actor_display_login,
    payload.ref_type AS ref_type,
    type,
    created_at,
    year(created_at) AS created_year,
    month(created_at) AS created_month,
    dayofmonth(created_at) AS created_dayofmonth
  FROM ghactivity
  WHERE type = 'CreateEvent'
    AND payload.ref_type = 'repository'
""")

new_repos. \
  write. \
  partitionBy('created_year', 'created_month', 'created_dayofmonth'). \
  mode('overwrite'). \
  parquet('s3://aigithub/github_dm/new_repos')
```
* Create a new IAM Role with permissions on s3 bucket for Redshift cluster.
* Come up with the logic to write dataframe to Redshift Table.
```
import boto3
import json
sm_client = boto3.client('secretsmanager')
secret_value = sm_client.get_secret_value(SecretId='demo/github/redshift')
credentials = json.loads(secret_value['SecretString'])

username = credentials['username']
password = credentials['password']
host = credentials['host']
port = credentials['port']
database = 'github_dm'
url = f"jdbc:redshift://{host}:{port}/{database}?user={username}&password={password}"

new_repos. \
    write. \
    mode('append'). \
    format('io.github.spark_redshift_community.spark.redshift'). \
    option(
        'aws_iam_role', 
        'arn:aws:iam::269066542444:role/service-role/AmazonRedshift-CommandsAccessRole-20220625T110940'
    ). \
    option('url', url). \
    option('dbtable', 'public.ghrepos'). \
    option('tempdir', 's3://aigithub/temp/ghrepos'). \
    save()
```
* Come up with the logic to write dataframe s3 and then use COPY to copy data to Redshift Table.
```
new_repos. \
    write. \
    mode('overwrite'). \
    parquet('s3://aigithub/github_dm/ghrepos')

import boto3
import json
sm_client = boto3.client('secretsmanager')
secret_value = sm_client.get_secret_value(SecretId='demo/github/redshift')
credentials = json.loads(secret_value['SecretString'])

username = credentials['username']
password = credentials['password']
host = credentials['host']
port = credentials['port']
database = 'github_dm'

import psycopg2
conn = psycopg2.connect(
    host=host,
    port=port,
    database='github_dm',
    user=username,
    password=password
)

cur = conn.cursor()

s3_bucket = 's3://aigithub/github_dm/ghrepos'
aws_iam_role = 'arn:aws:iam::269066542444:role/service-role/AmazonRedshift-CommandsAccessRole-20220625T110940'
table = 'ghrepos'

cur.execute(f'TRUNCATE TABLE {table}')
copy_command = f'''COPY {table}
    FROM '{s3_bucket}'
    IAM_ROLE '{aws_iam_role}'
    FORMAT AS PARQUET
'''

cur.execute(copy_command)
```
