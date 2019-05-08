package com.imooc.java.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 求每个单词分值的平均值 input1.txt
 * spark 2
 * hadoop 3
 * kafka 5
 * storm 8
 * storm 2
 * spark 7
 * hadoop 3
 * spark 2
 * kafka 10
 * apache 3
 * kafka 30
 * kafka 100
 * storm 20
 * spark 9
 * spark 20
 * spark 100
 * hadoop 2
 * hadoop 90
 * hadoop 1
 * storm 6
 * kafka 15
 */
public class SparkMapAvg {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<String > df = spark.read().textFile("D:/spark_data/input1.txt");


        JavaRDD<String> rdd = df.javaRDD();

        //PairFlatMapFunction 三个参数为：输入 输出pair的key，输出pair的value
        JavaPairRDD<String, Tuple2<Integer,Integer>> javaPairRDD = rdd.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<Integer,Integer>>() {
            public Iterator<Tuple2<String, Tuple2<Integer,Integer>>> call(String s) throws Exception {
                String[] strs = s.split(" ");
                ArrayList<Tuple2<String, Tuple2<Integer,Integer>>> list = new ArrayList<Tuple2<String, Tuple2<Integer,Integer>>>();
                for (String str : strs) {
                    Tuple2<String, Tuple2<Integer,Integer>> tuple2 = new Tuple2<String, Tuple2<Integer,Integer>>(strs[0], new Tuple2<Integer, Integer>(Integer.valueOf(strs[1]), 1));
                    list.add(tuple2);
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                return new Tuple2<Integer, Integer>(x._1 + y._1,x._2+y._2);
            }
        });
        JavaPairRDD<String, Integer> result = javaPairRDD.mapValues(x -> {
            return x._1() / x._2();
        });

        System.out.println(result.collect());


    }
}
