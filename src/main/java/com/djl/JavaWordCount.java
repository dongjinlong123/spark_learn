package com.djl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaWordCount {
    public static void main(String[] args) {
        //创建sparkConf
        SparkConf sf = new SparkConf().setAppName("java_app");//.setMaster("local");
        //创建java的spark程序执行入口
        JavaSparkContext sc = new JavaSparkContext(sf);
        JavaRDD<String> line = sc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");
                List<String> list = Arrays.asList(words);
                return list.iterator();
            }
        });
        //单词和1组装一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //排序 ,java的RDD只支持sortByKey
        //调换单词和次数的位置
        JavaPairRDD<Integer, String> reduce_swap = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();//调换下顺序
            }
        });
        JavaPairRDD<Integer, String> ret_swap = reduce_swap.sortByKey(true);
        JavaPairRDD<String, Integer> ret = ret_swap.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        //保存并且释放资源
        ret.saveAsTextFile(args[1]);
        sc.close();
    }
}
