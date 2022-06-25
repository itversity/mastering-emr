export BUCKET_NAME=aigithub
export FOLDER=landing/ghactivity
export FILE_PATTERN=2022-06-19
export SECRET_ID=demo/github/redshift
export AWS_REDSHIFT_IAM_ROLE=arn:aws:iam::269066542444:role/service-role/AmazonRedshift-CommandsAccessRole-20220625T110940

spark-submit \
    --master yarn \
    --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
    app.py