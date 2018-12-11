package com.djl.spark_sql.udf

import java.io.File

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 先对日志进行过滤处理
  */
object AccessLog002 {
  val out_dir ="E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\src\\main\\scala\\com\\djl\\spark_sql\\access_out_data"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("AccessLog002").getOrCreate()
    val access: Dataset[String] = spark.read.textFile("E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\src\\main\\scala\\com\\djl\\spark\\access.log")
    import spark.implicits._
    val accessRd: Dataset[(String, String)] = access.map(x => {
      val fields = x.split("[|]")
      val time = fields(0)
      val ip = fields(1)
      (time, ip)
    })
    val df: DataFrame = accessRd.toDF("time","id")
    val f = new File(out_dir)
    f.deleteOnExit()
    df.write.parquet(out_dir)
  }
}
