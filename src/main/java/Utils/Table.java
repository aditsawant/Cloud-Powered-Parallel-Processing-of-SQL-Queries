package Utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Table {
    private static JSONObject mappingJSON;
    public ArrayList<ArrayList<Object>> table;
    public HashMap<String, Integer> aggMap;
    private String tableName;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTable(ArrayList<ArrayList<Object>> table) {
        this.table = table;
    }

    private HashMap<String, ArrayList<ArrayList<Object>>> groupByMap;

    public void setGroupByMap(HashMap<String, ArrayList<ArrayList<Object>>> groupByMap) {
        this.groupByMap = groupByMap;
    }

    public HashMap<String, ArrayList<ArrayList<Object>>> getGroupByMap() {
        return groupByMap;
    }

    public String getTableName() {
        return tableName;
    }

    public static JSONObject getMappingJSON() {
        return mappingJSON;
    }

    public Table() {
        this.tableName = "users";
        table = new ArrayList<>();
        JSONParser parser = new JSONParser();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader(".\\ColumnIndexMapping.json"));
            mappingJSON = new JSONObject(jsonTokener);
//            Object obj = parser.parse(new FileReader(".\\ColumnIndexMapping.json"));
//            mappingJSON = (JSONObject) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Object getColumnValue(String columnName){return null;}
    String groupByString(String[] columns){return null;}
    Boolean checkColumnValue(String columnName, String value){return null;}
    Object getAggregate(String operation, String column, ArrayList<Table> arr){return null;}
    Boolean compareAggregate(String column, String operation, String comparisonOperator, String value, ArrayList<Table> arr){return null;}

    void dropColumn(String columnName){
        for(ArrayList<Object> row: this.table){
            JSONObject tableJSON = (JSONObject) mappingJSON.get(tableName);
            row.remove(tableJSON.get(columnName));
        }
    }
}
