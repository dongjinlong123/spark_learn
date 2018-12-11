package com.djl.spark_sql

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *  使用DataSet 的sql 编写wordcount
  */
object DataSetSqlWD007 {
  def main(args: Array[String]): Unit = {
    //获取sprakSession
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataSetSqlWD2")
      .getOrCreate()
    //导入sparkSession 对象中的隐式转换
    import spark.implicits._
    //读取文件 spark.read.format("text").load("./wordcountData.txt") 得到DataFrame
    val df: DataFrame = spark.read.format("text").load("./wordcountData.txt")

    //DataFrame 封装的是Row对象
    val words: Dataset[String] = df.flatMap(_.getAs[String]("value").split(" "))

    //使用DSL风格 导入函数
    import org.apache.spark.sql.functions._
    val ret: Dataset[Row] = words.groupBy($"value".as("word"))
      .agg(count("*") //.agg执行聚合函数
        .name("counts"))
      .sort($"counts" desc)
    ret.show()
    //使用DataFrame 的sql 将DataSet 转化成DataFrame

    //可以使用DataSet 的withColumnRenamed 修改列名
    //也可以使用toDF(修改列名)
    val df_up: DataFrame = words.withColumnRenamed("value","word")
    val df_sql: DataFrame = df.toDF()
    df_sql.createTempView("djl_tmp")
    val ret_sql: DataFrame = spark.sql("select word ,count(*) counts from djl_tmp group by word order by counts desc")
    ret_sql.show()
    //ret_sql.write.csv("/out")
//    ret_sql.write.json("/out")
    //ret_sql.write.parquet("out") //列方式存储的格式（基于列压缩）
    //结果写入mysql中
//    ret_sql.write.jdbc("url","tableName",new Properties())
//    ret_sql.foreachPartition(x=>{
//      dataToMysql(x)
//    })
  }
  def dataToMysql(part: Iterator[Row]):Unit={
    part.foreach(row=>{
      val con:Connection=null;
      val word: String = row.getAs[String]("word")
      val counts: Int = row.getAs[Int]("counts")
    })
  }
}
