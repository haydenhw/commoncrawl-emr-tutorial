package com.revature.emrtemplate

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object RandomSample {
    val spark = SparkSession
      .builder()
      .appName("random sample")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    val crawl = "CC-MAIN-2020-05"
    val jobUrls = df
      .select("url_host_name", "url_path")
      .filter($"crawl" === crawl )
      .filter($"subset" === "warc")
      .filter($"url_path".contains("job"))

    val seed = 5
    val withReplacement = false
    val fraction = 0.007
    val sample = jobUrls.sample(withReplacement, fraction, seed).limit(1000000)

    val s3bucket =
      "s3a://emr-output-revusf/joburls" + crawl + System.currentTimeMillis() + "rsample"

    sample.write.format("csv").mode("overwrite").save(s3bucket)

    // val rdd = spark.sparkContext.parallelize(jobUrlsArr)
    // rdd.map(_.mkString(",")).saveAsTextFile(s3bucket)

    spark.close
}
