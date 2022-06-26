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
