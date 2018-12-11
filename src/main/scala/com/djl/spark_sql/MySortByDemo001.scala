package com.djl.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义的排序规则
  */
object MySortByDemo001 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySortByDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // id,name,age,颜值
    val data = List("1,张三,21,90", "2,李四,23,35", "3,赵七,22,35", "4,djl,26,99")
    //按照颜值大到小，相同则按年龄
    val lines = sc.parallelize(data)
    val userRDD: RDD[(Long, String, Int, Int)] = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      (id, name, age, fv)
    }).cache()
    val ret: RDD[(Long, String, Int, Int)] = userRDD.sortBy(x => {
      //java对象实现了Comparable 接口
      new UserSortDTO(x._1, x._2, x._3, x._4)
    }, false)
    println(ret.collect().toBuffer)
    val ret2: RDD[(Long, String, Int, Int)] = userRDD.sortBy(x => {
      //scala对象实现了Comparable 接口,不改变数据，只改变顺序，可以只封装需要排序的字段
      //不用所有参数都传
      User(x._1, x._2, x._3, x._4)
    }, false)
    println(ret2.collect().toBuffer)
    sc.stop()
  }

  /*case 描述类相当于静态类*/
  case class User(val id: Long, val name: String, val age: Int, val fv: Int) extends Comparable[User] {
    override def compareTo(o: User): Int = {
      if (this.fv == o.fv) {
        o.age - this.age
      } else {
        this.fv - o.fv
      }
    }
  }

}
