package com.imooc.java.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 数据样例input2.txt：
 * 1 3 5 7 9
 * 2 4 6 8 10
 */
public class RddSum {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<String> rdd = spark.read().textFile("D:/spark_data/input2.txt").javaRDD();
        JavaRDD<Integer> rdd1 = rdd.flatMap(line -> {//先每个元素拆散生成rdd
            String[] s = line.split(" ");
            List<Integer> list = new ArrayList<Integer>();
            Arrays.stream(s).forEach(x -> {
                list.add(Integer.valueOf(x));
            });
            return list.iterator();
        });
        Integer sum = rdd1.reduce((x,y) ->{//reduce求和
           return x +y;
        });
        System.out.println(sum / rdd1.count() );

    }
}
