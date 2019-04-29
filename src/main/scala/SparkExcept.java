import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkExcept {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("except").getOrCreate();

        Dataset<Row> p1 = spark.read().json("D:/spark_data/people.json");
        Dataset<Row> p2 = spark.read().json("D:/spark_data/people2.json");
//        p1.except(p2).foreach(new ForeachFunction<Row>() {//p1中删除掉p2中出现的部分
//            public void call(Row row) throws Exception {
//                System.out.println(row.mkString());
//            }
//        });
//        p1.union(p2).distinct().foreach(new ForeachFunction<Row>() {//p1和p2求并集，并用distinct去重
//            public void call(Row row) throws Exception {
//                System.out.println(row.get(0) + ":" + row.get(1));
//            }
//        });

//        p1.intersect(p2).foreach(new ForeachFunction<Row>() {//p1和p2求交集
//            public void call(Row row) throws Exception {
//                System.out.println(row.get(0) + ":" + row.get(1));
//            }
//        });

        Dataset<Person> as = p1.as(Encoders.bean(Person.class));
        as.foreach(new ForeachFunction<Person>() {
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

    }
}
