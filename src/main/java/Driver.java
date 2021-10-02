import Spark.SparkExecutor;
import Utils.SQLExecutor;
import Utils.SQLExecutor.*;
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
            System.out.println("An error occurred.");
            e.printStackTrace();
        }


//        File file = new File("D:\\IntelliJ\\Cloud_Assignment\\data\\input\\query.txt");
//        BufferedReader br = new BufferedReader(new FileReader(file));
//        while (br.readLine() != null) {
//            queries.add(br.readLine());
//        }
        // 2.csv
        //call Spark
//        for(String q: queries){
//            System.out.println(q);
//        }
        SparkExecutor.SparkDriver(queries);
        //call Hadoop + pass dataset
        //call Storm
    }

}
