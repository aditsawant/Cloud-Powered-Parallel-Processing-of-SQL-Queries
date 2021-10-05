package Spark;

import Utils.SQLExecutor;
import Utils.SQLQueries;
import Utils.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.json.JSON;

import java.util.*;

import static Utils.SQLExecutor.parseSQL;

public class SparkExecutor {
    public static HashMap<String, String[]> headers = new HashMap<>();
    private static final Logger log =
            LoggerFactory.getLogger(SparkExecutor.class);
    public static JSONObject queryJSON;
    public static String sqlQuery;
    private static long time_ms;

    public static void SparkDriver(ArrayList<String> queries){
        SparkExecutor app = new SparkExecutor();
        JSONArray outputTimes = new JSONArray();
        for(String query: queries){
            SparkExecutor.sqlQuery = query;
//            JSONObject ogQueryJSON = SQLExecutor.parseSQL(query.toLowerCase());
//            ArrayList<String> finalCols = new ArrayList<>();
//            for(Object col: (JSONArray) ogQueryJSON.get("columns")){
//                if(col.toString().equalsIgnoreCase("*")){
//                    finalCols.add(col.toString());
//                } else finalCols.add(col.toString().split("\\.")[1]);
//            }
//            ogQueryJSON.put("columns", finalCols);
//            SparkExecutor.queryJSON = ogQueryJSON;
            SparkExecutor.queryJSON = SQLExecutor.parseSQL(query.toLowerCase());
            if(SparkExecutor.queryJSON.opt("having") != null){
                JSONObject havingJSON = (JSONObject) SparkExecutor.queryJSON.opt("having");
                ((JSONObject) queryJSON.get("having")).put("column", ((String)havingJSON.get("column")).split("\\.")[1]);
            }
            if(SparkExecutor.queryJSON.opt("columns") != null){
                JSONArray newCols = new JSONArray();
                JSONArray cols = (JSONArray)SparkExecutor.queryJSON.get("columns");
                for(int i = 0; i < cols.length(); i++) {
                    if(!(cols.get(i) instanceof String)){
                        JSONObject a = (JSONObject) cols.get(i);
                        a.put("column", ((String) a.get("column")).split("\\.")[1]);
                        newCols.put(a);
                    } else {
                        String newColumn = (cols.get(i).toString()).split("\\.")[1];
                        newCols.put(newColumn);
                    }
                }
                SparkExecutor.queryJSON.put("columns", newCols);
            }
            if(SparkExecutor.queryJSON.opt("where") != null){
                ((JSONObject) SparkExecutor.queryJSON.get("where")).put("value1", ((String)((JSONObject) SparkExecutor.queryJSON.get("where")).get("value1")).split("\\.")[1]);
            }
            if(SparkExecutor.queryJSON.opt("groupByColumns") != null){
                JSONArray jArr = (JSONArray) SparkExecutor.queryJSON.get("groupByColumns");
                JSONArray newJArr = new JSONArray();
                newJArr.put(((String)jArr.get(0)).split("\\.")[1]);
                SparkExecutor.queryJSON.put("groupByColumns", newJArr);
            }
            System.out.println(query);
            System.out.println(SparkExecutor.queryJSON);
            app.start();

            JSONObject sparkSpec = new JSONObject();
            sparkSpec.put("Spark Execution Time in ms for query" + (queries.indexOf(query) + 1), time_ms);
            outputTimes.put(sparkSpec);
        }
        System.out.println(outputTimes);
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
    public void start() {
        // Creates a session on a local master
//        long start = new Date().getTime();

        System.out.println("Reached inside app.start");
        SparkSession spark = SparkSession.builder()
                .appName("Query Executor")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark session created.");
//        HashMap<String, String[]> headers = new HashMap<>();
        headers.put("movies", new String[]{"movieid", "title", "releasedate", "unknown", "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"});
        headers.put("users", new String[]{"userid", "age", "gender", "occupation", "zipcode"});
        headers.put("zipcodes", new String[]{"zipcode", "zipcodetype", "city", "state"});
        headers.put("rating", new String[]{"userid", "movieid", "rating", "timestamp"});
        headers.put("moviesXrating", new String[]{"movieid", "title", "releasedate", "unknown", "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western", "userid", "movieid", "rating", "timestamp"});
        headers.put("ratingXmovies", new String[]{"userid", "movieid", "rating", "timestamp", "movieid", "title", "releasedate", "unknown", "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"});
        headers.put("usersXzipcodes", new String[]{"userid", "age", "gender", "occupation", "zipcode", "zipcode", "zipcodetype", "city", "state"});
        headers.put("zipcodesXusers", new String[]{"zipcode", "zipcodetype", "city", "state", "userid", "age", "gender", "occupation", "zipcode"});
        headers.put("usersXrating", new String[]{"userid", "age", "gender", "occupation", "zipcode", "userid", "movieid", "rating", "timestamp"});
        headers.put("ratingXusers", new String[]{"userid", "movieid", "rating", "timestamp", "userid", "age", "gender", "occupation", "zipcode"});

        Table dataset = null;
        if(queryJSON.opt("joinType") == null) {
            Dataset<Row> df = spark.read().format("csv")
                    .option("header", true)
                    .option("inferSchema", true)
                    .load("data/input/".concat(SparkExecutor.queryJSON.getString("table")).concat(".csv"))
                    .toDF(headers.get(SparkExecutor.queryJSON.getString("table")));

            System.out.println("Spark read done.");
            df.createOrReplaceTempView(SparkExecutor.queryJSON.getString("table"));
            System.out.println("Going to run sparkDfToTable.");
            dataset = SparkExecutor.sparkDfToTable(df);
            dataset.setTableName((String) queryJSON.get("table"));
            System.out.println("Dataset created.");

        }
        else{
            Dataset<Row> df1 = spark.read().format("csv")
                    .option("header", true)
                    .option("inferSchema", true)
                    .load("data/input/".concat(((JSONObject)SparkExecutor.queryJSON.get("table")).getString("table1")).concat(".csv"))
                    .toDF(headers.get(((JSONObject)SparkExecutor.queryJSON.get("table")).getString("table1")));
            //read headers and add in arraylist
            Dataset<Row> df2 = spark.read().format("csv")
                    .option("header", true)
                    .option("inferSchema", true)
                    .load("data/input/".concat(((JSONObject)SparkExecutor.queryJSON.get("table")).getString("table2")).concat(".csv"))
                    .toDF(headers.get(((JSONObject)SparkExecutor.queryJSON.get("table")).getString("table2")));

            dataset = SQLQueries.join(queryJSON, SparkExecutor.sparkDfToTable(df1), SparkExecutor.sparkDfToTable(df2));
        }

        long start = new Date().getTime();
        if (dataset.table.isEmpty()) System.out.println("TABLE is empty initially!");
//        System.out.println(dataset.table.size());
        System.out.println("Init: " + dataset.table.get(0));

        if(queryJSON.opt("where") != null)
            SQLQueries.where(queryJSON, dataset);

        if(dataset.table.isEmpty()) System.out.println("TABLE is empty after where!");
//        System.out.println(dataset.table.size());
        System.out.println("Post where: " + dataset.table.get(0));

        if(queryJSON.opt("groupByColumns") != null)
            SQLQueries.groupBy(queryJSON, dataset);

        if(dataset.table.isEmpty()) System.out.println("TABLE is empty after groupby!");
//        System.out.println(dataset.table.size());
        System.out.println("Post group by: " + dataset.table.get(0));

        if(queryJSON.opt("having") != null)
            SQLQueries.having(queryJSON, dataset);

        if(dataset.table.isEmpty()) System.out.println("TABLE is empty after having!");
//        System.out.println(dataset.table.size());
        System.out.println("Post having: " + dataset.table.get(0));

        SQLQueries.select(queryJSON, dataset);

        System.out.println("Post select: " + dataset.table.get(0));
        System.out.println("select has been executed.");

        long end = new Date().getTime();

        if(dataset.table.isEmpty()) System.out.println("TABLE is empty!");
        for (ArrayList<Object> arr : dataset.table) {
            System.out.println(arr.toString());
        }
        System.out.println("Spark Execution Time "+(end-start) + " milliseconds");
        time_ms = end - start;
    }

    public static Table sparkDfToTable(Dataset<Row> df){
        ArrayList<ArrayList<Object>> table = new ArrayList<>();
        List<Row> rowList = df.collectAsList();

        for(Row row: rowList){
            ArrayList<Object> temp = new ArrayList<>();
            for(int i = 0; i < row.length(); i++){
                temp.add(row.get(i));
            }
            table.add(temp);
        }
        Table dataset = new Table();
        dataset.setTable(table);
        return dataset;
    }
}
