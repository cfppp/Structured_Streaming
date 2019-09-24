package com.atguigu.structure.streaming.day01.filesource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Filesource1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("hh")
      .master("local[*]")
      .getOrCreate()

    val userSchema: StructType = StructType(StructField("name",StringType)::StructField("age",IntegerType)::StructField("sex",StringType)::Nil)
    val inputFram = spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("C:\\Users\\17629227368\\Desktop\\test")

    inputFram.writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
      .awaitTermination()

  }
}
