package Storm;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileNotFoundException;
import java.io.FileReader;


public class WhereFilter extends BaseFilter {
    public boolean isKeep(TridentTuple tuple) {
        JSONObject queryJSON = new JSONObject();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader("query.json"));
            queryJSON = new JSONObject(jsonTokener);
        } catch (FileNotFoundException | JSONException e) {
            e.printStackTrace();
        }

        String value1 = null;
        try {
            value1 = (String) ((JSONObject) queryJSON.get("where")).get("value1");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        String value2 = null;
        try {
            value2 = (String) ((JSONObject) queryJSON.get("where")).get("value2");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        int field_index = tuple.fieldIndex(value1);
        String operator = null;
        try {
            operator = (String) ((JSONObject) queryJSON.get("where")).get("operator");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        if(operator.equals("=")) {
            String value1_trimmed = tuple.getString(field_index).replaceAll("\"","");
            return value1_trimmed.equalsIgnoreCase(value2);
        }
        else if(operator.equals(">")) {
            return Integer.parseInt(tuple.getString(field_index)) > Integer.parseInt(value2);
        }
        else if(operator.equals("<")) {
            return Integer.parseInt(tuple.getString(field_index)) < Integer.parseInt(value2);
        }
        else if(operator.equals(">=")) {
            return Integer.parseInt(tuple.getString(field_index)) >= Integer.parseInt(value2);
        }
        else if(operator.equals("<=")) {
            return Integer.parseInt(tuple.getString(field_index)) <= Integer.parseInt(value2);
        }
        else if(operator.equals("<>")) {
            return Integer.parseInt(tuple.getString(field_index)) != Integer.parseInt(value2);
        }
        else if(operator.equals("like")) {
            String value1_string = tuple.getString(field_index).replaceAll("\"","");
            value2 = value2.toLowerCase();
            value2 = value2.replace(".", "\\.");
            value2 = value2.replace("?", ".");
            value2 = value2.replace("%", ".*");
            value1_string = value1_string.toLowerCase();
            return value1_string.matches(value2);
        }
        else {
            String value1_string = tuple.getString(field_index).replaceAll("\"","");
            String[] string_array = value2.substring(1, value2.length() - 1).split(",");
            for(String s : string_array) {
                if(value1_string.equals(s.trim())) {
                    return true;
                }
            }
            return false;
        }

    }
}


