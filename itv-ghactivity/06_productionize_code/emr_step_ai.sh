--master yarn --deploy-mode cluster --conf "spark.yarn.appMasterEnv.ENVIRON=PROD" --conf "spark.yarn.appMasterEnv.SRC_DIR=s3://aigithub/landing/ghactivity" --conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" --conf "spark.yarn.appMasterEnv.TGT_DIR=s3://aigithub/emrraw/ghactivity" --conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" --conf "spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2022-06-15" --py-files s3://aigithub/app/itv-ghactivity.zip
 
s3://aigithub/app/app.py
