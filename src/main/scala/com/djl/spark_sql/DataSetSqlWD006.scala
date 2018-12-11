package com.djl.spark_sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  使用DataSet 的sql 编写wordcount
  */
object DataSetSqlWD006 {
  def main(args: Array[String]): Unit = {
    //获取sprakSession
    val spark = SparkSession.builder().master("local[*]")
      .appName("DataSetSqlWD")
      .getOrCreate()

    //读取文件   底层spark.read.format("text").load("./wordcountData.txt")
    val wd: Dataset[String] = spark.read.textFile("./wordcountData.txt")

    //导入sparkSession 对象中的隐式转换
    import spark.implicits._

    val words: Dataset[String] = wd.flatMap(_.split(" "))

    //DSL 修改列名 转换成DataFrame
    val df: DataFrame = words.withColumnRenamed("value","word")

    //执行sql 先创建临时视图，再执行sql
    df.createTempView("wd")
    val ret: DataFrame = spark.sql("select word,count(1) as counts from wd group by word order by counts desc")
    ret.show()
  }
}
