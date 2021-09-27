package Spark;

import Utils.SQLQueries;
import Utils.Table;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static Utils.SQLExecutor.parseSQL;

public class SparkExecutor {
    private static final Logger log =
            LoggerFactory.getLogger(SparkExecutor.class);
    private static JSONObject queryJSON;
    private static String sqlQuery;
    private static long time_ms;

    public static void main(String[] args) {
        SparkExecutor app = new SparkExecutor();
        SparkExecutor.sqlQuery = "select occupation from users where age = 20";
        SparkExecutor.queryJSON = parseSQL(SparkExecutor.sqlQuery.toLowerCase());
        System.out.println(sqlQuery);
        System.out.println(queryJSON);
        app.start();
    }

    public static JSONObject sparkOutput(String sqlQuery) {
        SparkExecutor app = new SparkExecutor();
        SparkExecutor.queryJSON = parseSQL(SparkExecutor.sqlQuery.toLowerCase());
        System.out.println(sqlQuery);
        System.out.println(queryJSON);
        app.start();

        JSONObject sparkSpec = new JSONObject();
        sparkSpec.put("Spark Execution Time in ms", new Long(time_ms));
        return sparkSpec;
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        long start = new Date().getTime();

        System.out.println("Reached inside app.start");
        SparkSession spark = SparkSession.builder()
                .appName("Query Executor")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark session created.");
        HashMap<String, String[]> headers = new HashMap<>();
        headers.put("movies", new String[]{"movieid", "title", "releasedate", "unknown", "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"});
        headers.put("users", new String[]{"userid", "age", "gender", "occupation", "zipcode"});
        headers.put("zipcodes", new String[]{"zipcode", "zipcodetype", "city", "state"});
        headers.put("rating", new String[]{"userid", "movieid", "rating", "timestamp"});

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/input/".concat(SparkExecutor.queryJSON.getString("table")).concat(".csv"))
                .toDF(headers.get(SparkExecutor.queryJSON.getString("table")));

        System.out.println("Spark read done.");
        // Calculating the orders info using SparkSQL
        df.createOrReplaceTempView(SparkExecutor.queryJSON.getString("table"));
//        df.show();
        System.out.println("Going to run sparkDfToTable.");
        Table dataset = SparkExecutor.sparkDfToTable(df);

        System.out.println("dataset created.");

        Table result = SQLQueries.where(queryJSON, dataset);
        result.setTableName((String)queryJSON.get("table"));
        result = SQLQueries.select(queryJSON, result);

        System.out.println("select has been executed.");
        for (ArrayList<Object> arr : result.table) {
            System.out.println(arr.toString());
        }

//        String sqlStatement = SparkExecutor.sqlQuery;

//        Dataset<Row> sqlDf = spark.sql(sqlStatement);
//        sqlDf.show();
        long end = new Date().getTime();
        System.out.println("Spark Execution Time "+(end-start) + "milliseconds");
        time_ms = end - start;
    }

    public static Table sparkDfToTable(Dataset<Row> df){
        ArrayList<ArrayList<Object>> table = new ArrayList<>();
        List<Row> rowList = df.collectAsList();

        for(Row row: rowList){
            ArrayList<Object> temp = new ArrayList<>();
            for(int i = 0; i < row.length(); i++){
//                System.out.print(row.get(i) + " ");
                temp.add(row.get(i));
            }
//            System.out.println("temp.size: " + temp.size());
//            System.out.println();
            table.add(temp);
//            System.out.println("table.size(): " + table.size());
        }

//        df.foreach((ForeachFunction<Row>) row -> {
//            ArrayList<Object> temp = new ArrayList<>();
//            for(int i = 0; i < row.length(); i++){
//                System.out.print(row.get(i) + " ");
//                temp.add(row.get(i));
//            }
////            System.out.println("temp.size: " + temp.size());
//            System.out.println();
//            table.add(temp);
//            System.out.println("table.size(): " + table.size());
//        });
        System.out.println("New table size: " + table.size());
        Table dataset = new Table();
        dataset.setTable(table);
        return dataset;
    }
}
