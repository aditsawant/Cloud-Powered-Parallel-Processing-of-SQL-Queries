package Storm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class SelectFilter extends BaseFunction {
	ArrayList<String> final_fields = new ArrayList<String>();
	String aggregateField = "";
	Map<String, Object> aggregateMap = new HashMap<String, Object>(); 
	
    public SelectFilter(ArrayList<String> columnNames, String aggregateField) {
    	this.final_fields = columnNames;
    	this.aggregateField = aggregateField;
    }
	
	
	   public SelectFilter(String[] tablefields, String aggregateField) {
		// TODO Auto-generated constructor stub
		   this.final_fields = (ArrayList<String>) Arrays.asList(tablefields);
		   this.aggregateField = aggregateField;
	}


	public void execute(TridentTuple tuple, TridentCollector collector) {
//		  String[] select_values;
//		  = (String[]) tuple.getValues().toArray(new String[0]);
//		  for(String s : this.final_fields) {
//			  
//		  }
//		for(Object o : tuple.select(new Fields(this.final_fields))) {
//			System.out.println(o.toString());
//		}
		
//		try(FileWriter fw = new FileWriter("output.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
//			    out.println(tuple.select(new Fields(this.final_fields)));
//			} catch (IOException e) {
//			    //exception handling left as an exercise for the reader
//			} 
		
		if(aggregateField.length() != 0)
		{
			aggregateMap.put((String) tuple.getValueByField(aggregateField), (TridentTuple) tuple);
//			System.out.println((TridentTuple) tuple);
		}
		else
		{
			try(FileWriter fw = new FileWriter("Storm_output.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
				out.println(tuple.select(new Fields(this.final_fields)));
			} catch (IOException e) {
			    //exception handling left as an exercise for the reader
			} 
		}
		collector.emit(new Values(tuple.select(new Fields(this.final_fields)))); 
		  
	   }
	
	public void cleanup() {
		FileWriter file = null;
		if(aggregateField.length() != 0)
		{
			JSONObject map_json = new JSONObject(this.aggregateMap);
	        try {
	        	 
	            // Constructs a FileWriter given a file name, using the platform's default charset
	        	file = new FileWriter("Storm_output.txt",true);
	            file.write(map_json.toString());
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            try {
	                file.flush();
	                file.close();
	            } catch (IOException e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }
	        }
		}
		
	}
	}

