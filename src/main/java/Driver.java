import Spark.SparkExecutor;
import java.util.*;
import java.io.*;

public class Driver {
    public static void main(String[] args) throws IOException {
        //File reading
        // 1.query
        ArrayList<String> queries = new ArrayList<>();
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

        // 2.csv
        //call Spark
        SparkExecutor.SparkDriver(queries);

        //call Hadoop + pass dataset
        //call Storm
    }

}
