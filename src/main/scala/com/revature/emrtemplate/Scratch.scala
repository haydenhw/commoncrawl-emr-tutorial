package com.revature.emrtemplate

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
object Scratch extends App {
  val spark = SparkSession
    .builder()
    .appName("1M Limit")
    .getOrCreate()
  val sc = spark.sparkContext

  val filePath = "/Users/hayden/p3data/wet/head.txt"

  val conf = new Configuration(sc.hadoopConfiguration)
  conf.set("textinputformat.record.delimiter", "WARC/1.0")
  val input = sc.newAPIHadoopFile(
    filePath,
    classOf[TextInputFormat],
    classOf[LongWritable],
    classOf[Text],
    conf
  )

  val lines = input
    .map({ case (longWritable, text) => text.toString })
    .foreach(record => {
      println("*********************")
      println(record)
      // val split = record.split("\n")
      // split.foreach(s => {
      //   println(s)
      // })
    })


  // println(
  //   "0: " + arr(0)
  // )

  // println(
  //   "1: " + arr(1)
  // )

  // println(
  //   "2: " + arr(2)
  // )

  // println(
  //   "3: " + arr(3)
  // )
}
