package com.atguigu.structure.streaming.day01

import org.apache.spark.sql.SparkSession

object kfksource1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hh")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
      .option("subscribe", "first")
      .load
      .selectExpr("cast(value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count
    df.write
      .format("console")
      .option("truncate",false)
      .save





  }
}
