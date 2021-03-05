## EMR Tutorial
This guide walks you through submitting a Scala Spark job to EMR that queries 500k job urls from Common Crawl and stores the result in an S3 bucket as CSV.

### Prerequisites
This isn't required, but I highly reccomend this 6 minute intro to EMR. It's the simplest EMR hello world you could ask for

https://www.youtube.com/watch?v=gOT7El8rMws&ab_channel=JohnnyChivers

### S3 Setup
You'll need to create two S3 buckets for this tutorial, an "input" bucket and an "output" bucket.
You'll use the "input" bucket to upload your Spark application jar file. 
The "output" bucket will store the output data produced by your application.

I named my two buckets `input-bucket-revusf` and `output-bucket-revusf`

```shell
s3cmd mb input-bucket-revusf
s3cmd mb output-bucket-revusf
```

I'm going to use `s3cmd` for throughout this guide, but feel free to use AWS-cli or the S3 console.

![](screenshots/1.5-change-bucekt-name.png)
![](screenshots/10-running-step.png)
![](screenshots/11-history-server.png)
![](screenshots/12-complete-step.png)
![](screenshots/13-download-data.png)
![](screenshots/14-display-data.png)
![](screenshots/2-sbt-assembly.png)
![](screenshots/3-upload-jar.png)
![](screenshots/4-create-cluster.png)
![](screenshots/5-step-execution.png)
![](screenshots/6-spark-application.png)
![](screenshots/7-configure-step.png)
![](screenshots/7.5-configured-step.png)
![](screenshots/8-finish-create-cluster.png)
![](screenshots/9-steps-tab.png)
![](screenshots/console-summary.png)
![](screenshots/messedup-7-configure-step.png)
![](screenshots/nouse-hsitory-server.png)
![](screenshots/spark-job-details.png) 
