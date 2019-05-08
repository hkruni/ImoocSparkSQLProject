package com.imooc.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkContextDemo1 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer > rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8),2);
        JavaRDD<Integer> rdd1 = rdd.mapPartitions(partition -> {
            System.out.println("分区");
            List<Integer> list = new ArrayList<Integer>();
            while (partition.hasNext()) {
                list.add(partition.next() * 2);
            }
            return list.iterator();
        });
        System.out.println(rdd1.collect());
    }
}
