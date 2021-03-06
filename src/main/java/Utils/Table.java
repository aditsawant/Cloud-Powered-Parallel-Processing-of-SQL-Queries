package Utils;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;


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
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader(".\\ColumnIndexMapping.json"));
            mappingJSON = new JSONObject(jsonTokener);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Object getColumnValue(String columnName){return null;}
    public String groupByString(String[] columns){return null;}
    public Boolean checkColumnValue(String columnName, String value){return null;}
    public Object getAggregate(String operation, String column, ArrayList<Table> arr){return null;}
    public Boolean compareAggregate(String column, String operation, String comparisonOperator, String value, ArrayList<Table> arr){return null;}

    void dropColumn(String columnName){
        for(ArrayList<Object> row: this.table){
            JSONObject tableJSON = (JSONObject) mappingJSON.get(tableName);
            row.remove(tableJSON.get(columnName));
        }
    }
}
