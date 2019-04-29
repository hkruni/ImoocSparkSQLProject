import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.List;

public class Sparksql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        Dataset<Row> df = spark.read().json("D:/spark_data/people.json");
//        df.write().format("parquet").mode(SaveMode.Append).save("D:/aaa");
//        df.registerTempTable("people");
//        df.foreach(new ForeachFunction<Row>() {
//            public void call(Row row) throws Exception {
//                System.out.println(row.getAs("age") + " " + row.getAs("name"));
//            }
//        });
//        df.show();//打印表格
//        df.printSchema();//打印schema
        //System.out.println(df.count());//打印数据行数
//        df.select("name","age").show();//只查询name和age两列
//        df.select(df.col("name"),df.col("age")).show();//同上
//        df.filter(df.col("age").geq(10)).select("name").show();//过滤出age>10的
//        df.filter(df.col("age").geq(10)).select("name").foreach(new ForeachFunction<Row>() {
//            public void call(Row row) throws Exception {
//                System.out.println(row.get(0));
//            }
//        });
//        df.sort(df.col("age").desc()).show();//age降序排序
//        df.groupBy("age").count().show();//按照age分组并统计每组个数
//        df.groupBy("name").max("age").show();//统计每组年龄最大值
//        df.groupBy("name").avg("age").show();//统计每组年龄平均值


//        Dataset<Row> sql = spark.sql("select name from people where age > 20");
//        sql.show();

//        List<Person> list = df.as(Encoders.bean(Person.class)).collectAsList();
//        for (Person person : list) {
//            System.out.println(person);
//        }



        //从数据库加载数据
        Dataset<Row> news = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/news")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "news")
                .option("user", "root")
                .option("password", "root").load();

        Dataset<Row> newsmodules = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/news")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "newsmodules")
                .option("user", "root")
                .option("password", "root").load();
        news.registerTempTable("news");
        newsmodules.registerTempTable("newsmodules");
        Dataset<Count> ds = spark.sql("select m.name,count(1) as count\n" +
                "from newsmodules m left join news n on m.id=n.module_id\n" +
                "GROUP BY m.name").as(Encoders.bean(Count.class));
        ds.foreach(new ForeachFunction<Count>() {
            public void call(Count count) throws Exception {
                System.out.println(count);
            }
        });


//        spark.sql("select * from news where title  like  '美%'").show();
//        spark.sql("select * from news where news_time  >  '2019-04-10'").show();


    }
}
