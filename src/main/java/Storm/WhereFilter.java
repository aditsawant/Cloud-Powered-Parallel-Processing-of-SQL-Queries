package demo.trident_demo;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class WhereFilter extends BaseFilter {
	
    public boolean isKeep(TridentTuple tuple) {
//        JSONParser parser = new JSONParser();
        JSONObject queryJSON = new JSONObject();
        try {
//            Object obj = parser.parse(new FileReader("C:\\Users\\Maithil\\Desktop\\eclipse\\workspace\\trident_demo\\src\\main\\java\\demo\\trident_demo\\query.json"));
//            queryJSON =  (JSONObject) obj;
            JSONTokener jsonTokener = new JSONTokener(new FileReader("C:\\Users\\Maithil\\Desktop\\eclipse\\workspace\\trident_demo\\src\\main\\java\\demo\\trident_demo\\query.json"));
            queryJSON = new JSONObject(jsonTokener);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
//        for(Object o : tuple.getValues()) {
//        	System.out.println(o);
//        }

        String value1 = null;
		try {
			value1 = (String) ((JSONObject) queryJSON.get("where")).get("value1");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        String value2 = null;
		try {
			value2 = (String) ((JSONObject) queryJSON.get("where")).get("value2");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int field_index = tuple.fieldIndex(value1);
        String operator = null;
		try {
			operator = (String) ((JSONObject) queryJSON.get("where")).get("operator");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        if(operator.equals("=")) {
        		String value1_trimmed = tuple.getString(field_index).replaceAll("\"","");
//        		System.out.println(value1_trimmed);
//        		System.out.println(value2);
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
                return tuple.getString(field_index).contains(value2);
            }
        else {
        	return tuple.getString(field_index).contains(value2);
        	//Write IN logic
            }

        }
    }



