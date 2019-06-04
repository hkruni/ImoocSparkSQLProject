package com.imooc.java.spark;

import com.imooc.java.po.Count;
import com.imooc.java.po.Person;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * 从parquet读取数据，并转换成bean
 */
public class SparkParquet {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Person> df = spark.read().parquet("D:/aaa").as(Encoders.bean(Person.class));
        Dataset<Count> df_count = spark.read().parquet("D:/data-count-parquet").as(Encoders.bean(Count.class));


//        df.foreach(new ForeachFunction<Person>() {
//            public void call(Person person) throws Exception {
//                System.out.println(person.toString());
//            }
//        });

        df_count.foreach(new ForeachFunction<Count>() {
            public void call(Count count) throws Exception {
                System.out.println(count.toString());
            }
        });
    }
}
