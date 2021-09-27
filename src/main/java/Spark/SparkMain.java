package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMain {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .config("spark.eventLog.enabled", "false")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().csv("D:\\IntelliJ\\Cloud_Assignment_1\\CC_Ass1_dataset\\movies.csv");
        dataset.show();
        System.out.println("Hello");
        spark.stop();
    }
}