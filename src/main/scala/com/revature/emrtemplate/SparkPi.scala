package com.revature.emrtemplate

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object Runner {
  def main(args: Array[String]) {
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
      .filter($"crawl" === crawl)
      .filter($"subset" === "warc")
      .filter($"url_path".contains("job"))

    val seed = 5
    val withReplacement = false
    val fraction = 0.07
    val sample = jobUrls.sample(withReplacement, fraction, seed).limit(1000000)

    val outputBucket =
      s"s3a://emr-output-revusf/joburls_${crawl}_${System.currentTimeMillis()} + randomsample"

    sample.write.format("csv").mode("overwrite").save(outputBucket)

    spark.close
  }
}
