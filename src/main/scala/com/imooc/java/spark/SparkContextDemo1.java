package com.imooc.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkContextDemo1 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String > rdd = sc.parallelize(Arrays.asList("one","two","three"));
        System.out.println(rdd.count());

        for (String s : rdd.take(3)) {
            System.out.println(s);
        }
    }
}
