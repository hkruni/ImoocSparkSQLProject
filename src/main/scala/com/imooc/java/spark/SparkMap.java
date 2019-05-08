package com.imooc.java.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 统计各个单词出现的次数，input1.txt的数据
 * INFO This is a message with content
 * INFO This is some other content
 *
 * INFO Here are more messages
 * WARN This is a warning
 *
 * ERROR Something bad happened
 * WARN More details on the bad thing
 * INFO back to normal messages
 */
public class SparkMap {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        Dataset<String > df = spark.read().textFile("D:/spark_data/input.txt");
//        Dataset<String > df1 = df.filter(new FilterFunction<String>() {//先对空行进行过滤
//            public boolean call(String s) throws Exception {
//                return s != null && s.length() > 0;
//            }
//        });
//        Dataset<String > df2 = df1.flatMap(new FlatMapFunction<String, String>() {//统计单词个数
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        }, Encoders.STRING());
//        System.out.println(df2.count());


        JavaPairRDD<String, Integer> rdd = spark.read().textFile("D:/spark_data/input.txt").toJavaRDD()
                .filter(line -> {//先过滤剔除空行
                    return line != null && line.length() > 0;
                }
        ).flatMapToPair(line -> {//每行拆分每个元素生成(元素名称 1)的pair
            String[] strs = line.split(" ");
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
            for (String str : strs) {
                Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>(str, 1);
                list.add(tuple2);
            }
            return list.iterator();
        }).reduceByKey((x,y) ->{//对相同key的pair的value进行求和
            return x +y;
        });


        System.out.println(rdd.collect());//把这些map变成list集合

    }
}
