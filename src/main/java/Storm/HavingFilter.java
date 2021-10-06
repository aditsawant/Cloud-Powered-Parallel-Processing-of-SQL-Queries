package Storm;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class HavingFilter extends BaseFilter{
	   public boolean isKeep(TridentTuple tuple) {
//		   return true;
	        JSONObject queryJSON = new JSONObject();
	        try {
	            JSONTokener jsonTokener = new JSONTokener(new FileReader("query.json"));
	            queryJSON = new JSONObject(jsonTokener);
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        

	        String value1 = null;
			try {
				value1 = ((String) ((JSONObject) queryJSON.get("having")).get("function")) + "(" + ((String) ((JSONObject) queryJSON.get("having")).get("column")) + ")";
//				System.out.println(value1);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	       
	        String value2 = null;
			try {
				value2 = (String) ((JSONObject) queryJSON.get("having")).get("value");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        int field_index = tuple.fieldIndex(value1);
	        String operator = null;
			try {
				operator = (String) ((JSONObject) queryJSON.get("having")).get("operator");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	        if(operator.equals("=")) {
	        		return Integer.parseInt((String) tuple.getValue(field_index).toString()) == Integer.parseInt(value2);
	            }
	        else if(operator.equals(">")) {
	                return (Integer.parseInt((String) tuple.getValue(field_index).toString()) > Integer.parseInt(value2));
	            }
	        else if(operator.equals("<")) {
                return (Integer.parseInt((String) tuple.getValue(field_index).toString()) < Integer.parseInt(value2));
	            }
	        else if(operator.equals(">=")) {
	        	System.out.println(Integer.parseInt((String) tuple.getValue(field_index).toString()));
                return (Integer.parseInt((String) tuple.getValue(field_index).toString()) >= Integer.parseInt(value2));
	            }
	        else if(operator.equals("<=")) {
                return (Integer.parseInt((String) tuple.getValue(field_index).toString()) <= Integer.parseInt(value2));
	            }
	        else if(operator.equals("<>")) {
                return (Integer.parseInt((String) tuple.getValue(field_index).toString()) != Integer.parseInt(value2));
	            }
	        else {
	        		System.out.println("Incorrect operator");
	        		return false;
	        }

	        }
}
