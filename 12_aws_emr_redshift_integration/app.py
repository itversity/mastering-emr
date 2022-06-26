import os
import json

import boto3
from pyspark.sql import SparkSession


def get_new_repos(spark):
    bucket_name = os.environ.get('BUCKET_NAME')
    folder = os.environ.get('FOLDER')
    file_pattern = os.environ.get('FILE_PATTERN')
    ghactivity = spark.read.json(f's3://{bucket_name}/{folder}/{file_pattern}*')
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
    return new_repos


def get_jdbc_url():
    secret_id = os.environ.get('SECRET_ID')
    sm_client = boto3.client('secretsmanager')
    secret_value = sm_client.get_secret_value(SecretId=secret_id)
    credentials = json.loads(secret_value['SecretString'])

    username = credentials['username']
    password = credentials['password']
    host = credentials['host']
    port = credentials['port']
    database = 'github_dm'
    jdbc_url = f"jdbc:redshift://{host}:{port}/{database}?user={username}&password={password}"
    return jdbc_url


def write_to_redshift(new_repos, jdbc_url):
    bucket_name = os.environ.get('BUCKET_NAME')
    aws_redshift_iam_role = os.environ.get('AWS_REDSHIFT_IAM_ROLE')
    new_repos.write.mode('overwrite').save(f's3://{bucket_name}/parquet/ghrepos')
    # new_repos. \
    #     write. \
    #     mode('append'). \
    #     format('io.github.spark_redshift_community.spark.redshift'). \
    #     option(
    #         'aws_iam_role', 
    #         aws_redshift_iam_role
    #     ). \
    #     option('url', jdbc_url). \
    #     option('dbtable', 'public.ghrepos'). \
    #     option('tempdir', f's3://{bucket_name}/temp/ghrepos'). \
    #     save()


def main():
    spark = SparkSession. \
        builder. \
        appName('GitHub Data Processor'). \
        master('yarn'). \
        getOrCreate()
    new_repos = get_new_repos(spark)
    jdbc_url = get_jdbc_url()
    write_to_redshift(new_repos, jdbc_url)


if __name__ == '__main__':
    main()