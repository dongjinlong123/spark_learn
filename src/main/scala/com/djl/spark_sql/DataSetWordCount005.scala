package com.djl.spark_sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * 使用DataSet 的DSL 编写wordcount
  */
object DataSetWordCount005 {
  def main(args: Array[String]): Unit = {
    //获取sprakSession
    val session = SparkSession.builder().master("local[*]")
      .appName("DataSetWordCount")
      .getOrCreate()
    //导入session对象中的隐式转换,不然无法调用RDD上的算子
    import session.implicits._

    //读取文件  Dataset[Row] 就是DataFrame
    val wd: Dataset[String] = session.read.textFile("./wordcountData.txt")

    //切分数据，并压平
    val words: Dataset[String] = wd.flatMap(_.split(" "))

    //DSL模式，列名默认value  ，取别名word  ， Dataset 没有reduceByKey
    //导入sparkSql的函数
    import org.apache.spark.sql.functions._

    //计算wordcount
    val row: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts")
      .sort($"counts" desc)
    row.show()
  }
}
