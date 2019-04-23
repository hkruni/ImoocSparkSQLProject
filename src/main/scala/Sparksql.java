import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class Sparksql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("D:/spark_data/people.json");
        df.registerTempTable("people");
        //df.show();
        // df.printSchema();
//        df.select("name","age").show();
        // df.filter(df.col("age").geq(10)).show();
        //df.sort(df.col("age").desc()).show();
        //df.groupBy("age").count().show();
        // Dataset<Row> sql = spark.sql("select name from people where age > 20");
        //sql.show();
        Dataset<Row> load = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/news")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "news")
                .option("user", "root")
                .option("password", "root").load();
        //load.show();
        load.registerTempTable("news");
        //spark.sql("select * from news where title  like  '美%'").show();
        spark.sql("select * from news where news_time  >  '2019-04-10'").show();


    }
}
