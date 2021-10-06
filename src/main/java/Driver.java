import Spark.SparkExecutor;
import Utils.SQLExecutor;
import org.json.JSONObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.*;

public class Driver {
    public static void main(String[] args) throws IOException {
        //File reading
        // 1.query
        ArrayList<String> queries = new ArrayList<>();

        //reading queries from query.txt
        try {
            File myObj = new File("data/input/query.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                queries.add(data);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Query input file not found.");
            e.printStackTrace();
        }

        // Writing to query.json
        JSONObject queryJSON = SQLExecutor.parseSQL(queries.get(0).toLowerCase());
        FileWriter file = null;
        try {
            // Constructs a FileWriter given a file name, using the platform's default charset
            file = new FileWriter("query.json",false);
            file.write(queryJSON.toString(4));
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                file.flush();
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //run Spark
        SparkExecutor.SparkDriver(queries);

        // clear output.txt and run Storm
        Files.deleteIfExists(Paths.get("Storm_output.txt"));
        Storm.SQLExecutorTrident.StormDriver();

    }

}
