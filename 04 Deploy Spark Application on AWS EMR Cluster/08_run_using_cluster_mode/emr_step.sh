--master yarn --deploy-mode cluster --conf "spark.yarn.appMasterEnv.ENVIRON=PROD" --conf "spark.yarn.appMasterEnv.SRC_DIR=s3://itv-github/landing/ghactivity" --conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" --conf "spark.yarn.appMasterEnv.TGT_DIR=s3://itv-github/raw/ghactivity" --conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" --conf "spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2021-01-15" --py-files s3://itv-github/itv-ghactivity.zip
 
s3://itv-github/app.py
	
