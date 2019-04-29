package com.imooc.java.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 取每个单词的top3分数
 */
public class TopN {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<String> rdd = spark.read().textFile("D:/spark_data/input1.txt").javaRDD();
        JavaPairRDD<String, Iterable<Integer>> rdd1 = rdd.mapToPair(line -> {
            String[] s = line.split(" ");
            return new Tuple2<>(s[0], Integer.valueOf(s[1]));
        }).groupByKey();


    }

}
