package com.revature.emrtemplate

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object SimpleLimit {
    val spark = SparkSession
      .builder()
      .appName("1M Limit")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    val crawl = "CC-MAIN-2020-05"
    val jobUrls = df
      .select("url_host_name", "url_path")
      .filter($"crawl" === crawl)
      .filter($"subset" === "warc")
      .filter($"url_path".contains("job"))
      .limit(1000000)

    val s3bucket =
      "s3a://emr-output-revusf/joburls_" + crawl + "_" + System.currentTimeMillis() + "1MLIM"

    jobUrls.write.format("csv").mode("overwrite").save(s3bucket)

    // val rdd = spark.sparkContext.parallelize(jobUrlsArr)
    // rdd.map(_.mkString(",")).saveAsTextFile(s3bucket)

    spark.close
}
