# Data Pipeline using AWS Step Function with EMR

Here are the details related to the data pipeline that is going to be bult using AWS Step Function with AWS EMR.
* Make sure AWS Lambda Function is scheduled every 15 minutes to ingest GitHub Activity Data to s3.
* Create a lambda function which take bucket, prefix with date as arguments and return number of objects from s3. The function will return dict. The dict contain `ready_to_process` as key and value will be either True or False. If the number of objects is 24 then, it will be True else False.
* Add a task to the step function to invoke lambda function.
* Add conditional to the step function to confirm if data is supposed to be processed or not based on `ready_to_process`. Job should be forced to fail if `ready_to_process` is False.
* If `ready_to_process` is True, then we need to add steps to run Spark application to process the data.
    * Create EMR Cluster
    * Add Step to EMR Cluster
    * Terminate the EMR Cluster
* Finally the step functions should be marked as complete.
