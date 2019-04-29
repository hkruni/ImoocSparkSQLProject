package com.imooc.java.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RddSum {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<String> rdd = spark.read().textFile("D:/spark_data/input2.txt").javaRDD();
        JavaRDD<Integer> rdd1 = rdd.flatMap(line -> {
            String[] s = line.split(" ");
            List<Integer> list = new ArrayList<Integer>();
            Arrays.stream(s).forEach(x -> {
                list.add(Integer.valueOf(x));
            });
            return list.iterator();
        });
        Integer sum = rdd1.reduce((x,y) ->{
           return x +y;
        });
        System.out.println(sum / rdd1.count() );

    }
}
