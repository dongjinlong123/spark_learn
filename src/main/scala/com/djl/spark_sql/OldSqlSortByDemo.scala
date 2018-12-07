package com.djl.spark_sql


import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 使用spark-sql 1.6旧的API来自定义的排序规则
  */
object OldSqlSortByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySortByDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // id,name,age,颜值
    val data = List("1,张三,21,90", "2,李四,23,35", "3,赵七,22,35", "4,djl,26,99")
    //按照颜值大到小，相同则按年龄
    val lines = sc.parallelize(data)
    val ret = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      Person(id, name, age, fv)
    })

    //创建一个sqlContext   旧的API : spark1.6之前的方式
    val sqlContext = new SQLContext(sc)
    ////导入隐饰转换操作，否则RDD无法调用toDF方法
    import sqlContext.implicits._
    //将RDD转化成DataFrame
    val pdf = ret.toDF()
    //将dataframe注册成hive的表 ()
    pdf.registerTempTable("t_user")
    //sql方法是一个transformation 不会执行任务再生成一个DataFrame
    val result: DataFrame = sqlContext.sql("select * from t_user order by fv desc , age asc")
    //show 数据直接显示处理 是一个action
    result.show()
    //释放资源
    sc.stop()
  }
}

/*定义一个对象转换数据*/
case class Person(id: Long, name: String, age: Int, fv: Int);