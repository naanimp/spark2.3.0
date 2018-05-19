package com.mp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// define main method (Spark entry point)
object HelloWorld {
  def main(args: Array[String]) {

    // initialise spark context
//    val conf = new SparkConf().setAppName("HelloWorld")
//    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Filtering")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("data/filter_input_qa/tag_data_06_06_2017_17.txt").as[String]
    val words = data.map(value => {
      println("************" + value)
      val arr = value.split(":")
      arr
    })
    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    println("************")
    println("************")
    sparkSession.stop();
  }
}