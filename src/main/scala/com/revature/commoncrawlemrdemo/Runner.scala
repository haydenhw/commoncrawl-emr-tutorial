package com.revature.commoncrawlemrdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark._

/**
  * Spark job ready to be run on EMR
  * Finds 500k job urls from the common crawl columnar index
  * Stores the result as a CSV file on the S3 bucket of your choosing
  */

object Runner {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("commoncrawl emr demo")
      .getOrCreate()

    // Note: we're not providing any credentials or doing any s3 config here
    // EMR takes care of all of that for us

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    val crawl = "CC-MAIN-2020-05"
    val jobUrls = df
      .select("url_host_name", "url_path")
      .filter($"crawl" === crawl)
      .filter($"subset" === "warc")
      .filter($"url_path".contains("job"))
      .limit(500000)

    // Change YOUR-BUCKET-NAME to the name of the output bucket you created on S3
    val s3OutputBucket = "s3a://YOUR-BUCKET-NAME/commoncrawl-demo-data"

    jobUrls.write.format("csv").mode("overwrite").save(s3OutputBucket )

    spark.close
  }
}
