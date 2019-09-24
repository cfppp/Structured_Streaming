package com.atguigu.structure.streaming.day01.wordcount

import org.apache.spark.sql.SparkSession

object Wordcount1 {

  def main(args: Array[String]): Unit = {

    val ssc = SparkSession.builder().appName("hh").master("local[*]").getOrCreate()
    import ssc.implicits._

    ssc.readStream
      .format("socket")
      .option("host","hadoop102")
      .option("port",9999)
      .load


  }
}
