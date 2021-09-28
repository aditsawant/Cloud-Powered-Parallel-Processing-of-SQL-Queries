import java.io.*;
import java.util.*;

public class StormExecutor {
    public static Table stormToTable(Tuple tuple, String tableName) {
        //if statements for each type of table
        if(tableName.equals("Movies")) {
            Table stormTable = new movieTable();
            for (int i = 0; i < tuple.size(); i++) {
                stormTable.addRow(tuple.getString(i));
            }
        }
        else if(tableName.equals("Rating")){
            Table stormTable = new ratingTable(tuple);
            for(int i=0; i<tuple.size(); i++) {
                stormTable.addRow(tuple.getString(i));
            }
        }
        else if(tableName.equals("Users")){
            Table stormTable = new userTable(tuple);
            for(int i=0; i<tuple.size(); i++) {
                stormTable.addRow(tuple.getString(i));
            }
        }
        else {
            Table stormTable = new zipcodeTable(tuple);
            for(int i=0; i<tuple.size(); i++) {
                stormTable.addRow(tuple.getString(i));
            }
        }

    }

}