# README #

This project provides a Fluent utility Http client to interact with Spark Standalone Rest Server that is bundled with the Spark distribution, as described in Arthur Mkrtchyan's [blog post](http://arturmkrtchyan.com/apache-spark-hidden-rest-api).
 
# Features
- Submit jobs to a spark standalone cluster
- Inquire a job's current status that was previously submitted to the cluster
- Kill a job running on a cluster

# Maven Dependency

```xml
<dependency>
    <groupId>com.github.ywilkof</groupId>
    <artifactId>spark-jobs-rest-client</artifactId>
    <version>1.3.4</version>
</dependency>
```

# SBT
```
"com.github.ywilkof" % "spark-jobs-rest-client" % "1.3.4"
```

# Requirements
- JAVA 1.8
- Spark version supplying the Rest API. This client is compatabile with version 1.5 and above. 

## Creating the client

In order to issue requests to a Spark cluster, a client has to be created.
The client has several configurations, which will be used across all the requests issued from it.
Master host and Spark Version are required and the rest of the fields have sensible defaults.

``` java
SparkRestClient.builder()
    .masterHost("localhost")
    .sparkVersion("1.5.0")
.build();
```

By default, the client is backed up by an HttpClient with a BasicHttpClientConnectionManager.
This can be changed by supplying at client creation a different HttpClient, or by calling poolingHttpClient(int maxTotalConnections) which will override the default BasicHttpClientConnectionManager.
 
## Submitting Jobs

Submitting jobs issues a request to the cluster to start running a job across the cluster. 
If the submission works, a submissionId is returned for future references to that job.
Please note that a successful submission does not guarantee a successful start of your job.

Following is a basic submit request.

``` java 
final String submissionId = sparkRestClient.prepareJobSubmit()
    .appName("MySparkJob!")
    .appResource("file://path/to/my.jar")
    .mainClass("com.somejob.MyJob")
.submit();
```

A more detailed request can consist off addition app args needed by the job (such as environment),
paths to additional jars needed by the job and not supplied with the job jar and spark properties to tune and optimize 
the execution of the job.

## Inquiring Job Execution Status

Submitting this kind of request will return the Driver State of the job.
This is useful after submitting a job to recognize any fails in startup, or when you need to act upon
a successful finished execution of a job.

#### The possible states are:
- SUBMITTED
- RUNNING
- FINISHED
- RELAUNCHING 
- UNKNOWN
- KILLED
- FAILED
- ERROR
- QUEUED (Mesos)


Following is a basic job status request:

``` java 
final DriverState driverState = sparkRestClient
    .checkJobStatus()
    .withSubmissionId(submissionId);
```

If the location of the driver running is of interest, use `withSubmissionIdFullResponse`
which includes the ip and port of the worker executing the driver:

``` java 
JobStatusResponse jobStatus = 
   sparkRestClient
    .checkJobStatus()
    .withSubmissionIdFullResponse(submissionId);
    
 System.out.println(jobStatus.getWorkerHostPort());
```

## Killing A Job

Sometimes you might need to kill an unfinished job. For example, let's say you want to deploy a new version
of your job. You must first kill the running job and only then rerun, with the updated jar.

Following is a job kill request:

``` java 
final boolean successfulKill = sparkRestClient
    .killJob() 
    .withSubmissionId(submissionId);
```

The kill request returns a boolean that forwards the cluster response as to whether the kill request was successfully issued or not.
A kill request for a non-existing job will always return false.

##Known Issues
When using Spark's REST API, spark-default.conf is not being picked up. Simple solution to this is further elaborated on [my blog](http://www.yonatanwilkof.net/spark-rest-job-submit-api-environment-variable/).
# License
 
 Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
