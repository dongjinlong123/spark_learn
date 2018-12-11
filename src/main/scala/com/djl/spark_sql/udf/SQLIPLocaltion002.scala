package com.djl.spark_sql.udf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLIPLocaltion002 {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().appName("SQLIPLocation")
      .master("local[*]").getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //读取数据 ip规则
    val ruleLines: Dataset[String] = spark.read.textFile("E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\src\\main\\scala\\com\\djl\\spark\\ip.txt")
    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    //对ip规则进行整理
    val ruleDfs: Dataset[(Long, Long, String)] = ruleLines.map(x => {
      val fields: Array[String] = x.split("[|]")
      val ipStart = fields(2).toLong
      val ipEnd = fields(3).toLong
      val province = fields(6).toString
      (ipStart, ipEnd, province)
    })
    //收集到driver 端，然后广播出去
    val rule: Array[(Long, Long, String)] = ruleDfs.collect()
    //广播
    val broadcase: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rule)

    //读取已经处理好的parquet文件,自带了schema信息
    val df: DataFrame = spark.read.parquet(AccessLog002.out_dir)

    //注册视图
    df.createTempView("v_access")

    //注册自定义函数
    spark.udf.register("ip2Province",(ip:String) =>{
      //ip转换成十进制
      val ipnum: Long = SQLIPLocation001.ipToLong(ip)
      //判断ip在哪个身份中
      val iprules: Array[(Long, Long, String)] = broadcase.value
      //二分法进行查找
      val idx: Int = binarySerach(iprules,ipnum)
      iprules(idx)._3
    })

    //执行sql
    spark.sql("select ip2Province(id) as province ,count(*) counts from v_access group by ip2Province(id) limit 10").show()
  }

  /**
    * 二分法查找
    * @param lines
    * @param ip
    */
  def binarySerach(lines:Array[(Long, Long, String)],ip:Long): Int ={
    var starNum = 0
    var endNum = lines.length - 1
    while(starNum <=endNum){
      val middle = (starNum+endNum)/2
      if(ip >= lines(middle)._1 && ip <=lines(middle)._2){
        return middle
      }else if (ip < lines(middle)._1){
        endNum = middle -1
      }else{
        starNum = middle +1
      }
    }
    -1
  }
}
