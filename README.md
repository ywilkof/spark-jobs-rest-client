# README #

This project is provides a Fluent utility Http client to interact with Spark Standalone Rest Server that is bundled with the Spark distribution, as described in Arthur Mkrtchyan's [blog post](http://arturmkrtchyan.com/apache-spark-hidden-rest-api).
 
 
# Features
- Submit jobs to a spark standalone cluster
- Inquire a job's current status that was previously submitted to the cluster
- Kill a job running on a cluster
 
## Submitting Jobs

``` java 
final String submissionId = sparkRestClient.prepareJobSubmit()
.appName("MySparkJob!")
.appResource("file://path/to/my.jar")
.mainClass("com.somejob.MyJob")
.submit();
```
 
# License
 
 Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0