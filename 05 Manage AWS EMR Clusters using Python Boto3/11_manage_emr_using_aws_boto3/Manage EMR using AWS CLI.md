# Manage AWS EMR Cluster using AWS CLI

Here are the steps we are going to follow to understand how to manage EMR using AWS CLI. It includes how to deploy Spark Application as a step to running EMR Cluster.
* Create AWS EMR Cluster using AWS Web Console
* Export AWS CLI and create a script with the command
* Login into EMR Cluster and upload files to s3
* Confirm that the application zip file as well as driver program file are in s3
* Generate JSON File to deploy Spark Application as AWS EMR Step
* Run AWS CLI Command to add Spark Applciation as AWS EMR Step to running cluster
* Validate if AWS EMR Step is executed successfully or not.
* Make sure to terminate the AWS EMR Cluster