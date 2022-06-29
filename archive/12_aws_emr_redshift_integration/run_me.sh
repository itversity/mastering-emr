# export BUCKET_NAME=aigithub
# export FOLDER=landing/ghactivity
# export FILE_PATTERN=2022-06-19
# export SECRET_ID=demo/github/redshift
# export AWS_REDSHIFT_IAM_ROLE=arn:aws:iam::269066542444:role/service-role/AmazonRedshift-CommandsAccessRole-20220625T110940

spark-submit \
    --master yarn \
    --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
	--conf "spark.yarn.appMasterEnv.BUCKET_NAME=aigithub" \
	--conf "spark.yarn.appMasterEnv.FOLDER=landing/ghactivity" \
	--conf "spark.yarn.appMasterEnv.FILE_PATTERN=2022-06-19" \
	--conf "spark.yarn.appMasterEnv.SECRET_ID=demo/github/redshift" \
	--conf "spark.yarn.appMasterEnv.AWS_REDSHIFT_IAM_ROLE=arn:aws:iam::269066542444:role/service-role/AmazonRedshift-CommandsAccessRole-20220625T110940" \
    app.py