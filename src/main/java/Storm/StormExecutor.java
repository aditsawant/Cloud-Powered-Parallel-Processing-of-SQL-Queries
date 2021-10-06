package Storm;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.util.hash.Hash;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.JoinType;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Max;
import org.apache.storm.trident.operation.builtin.Min;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;
import scala.util.parsing.json.JSON;

import java.io.*;
import java.util.*;


public class StormExecutor {
    public static JSONObject stormJSON = new JSONObject();
    public static JSONObject StormDriver() throws IOException {
        JSONParser parser = new JSONParser();
        JSONObject mappingJSON = new JSONObject();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader("ColumnIndexMapping.json"));
            mappingJSON = new JSONObject(jsonTokener);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        JSONParser parser2 = new JSONParser();
        JSONObject queryJSON = new JSONObject();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader("query.json"));
            queryJSON = new JSONObject(jsonTokener);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
//        JSONObject table = (JSONObject) queryJSON.get("table");
        String[] fields = null;
        String table_name = null;
        TridentTopology sqlTopology = new TridentTopology();

        FeederBatchSpout movieSpout = new FeederBatchSpout(ImmutableList.of("movieid" , "title" , "releasedate" , "unknown" , "Action" , "Adventure" , "Animation" ,
                "Children" , "Comedy" , "Crime" , "Documentary" , "Drama" , "Fantasy" ,
                "Film_Noir" , "Horror" , "Musical" , "Mystery" , "Romance" , "Sci_Fi" ,
                "Thriller" , "War" , "Western"));
        FeederBatchSpout userSpout = new FeederBatchSpout(ImmutableList.of("userid" , "age" , "gender" , "occupation" , "zipcode"));
        FeederBatchSpout ratingSpout = new FeederBatchSpout(ImmutableList.of("userid" , "movieid" , "rating" , "timestamp"));
        FeederBatchSpout zipcodeSpout = new FeederBatchSpout(ImmutableList.of("zipcode" , "zipcodetype" , "city" , "state"));

        HashMap<String, Object> spoutMap = new HashMap<String, Object>();
        spoutMap.put("movies",movieSpout);
        spoutMap.put("users",userSpout);
        spoutMap.put("rating",ratingSpout);
        spoutMap.put("zipcodes",zipcodeSpout);
        
        Stream sqlTopologyStream = null;
        TridentState sqlTopologyTridentState = null;
        
        // Check if join is present
        if(queryJSON.has("joinType")){
            JSONObject table_json = (JSONObject) queryJSON.get("table");
            String table1_name = (String) table_json.get("table1");
            String table2_name = (String) table_json.get("table2");
            Set<String> fields1 = get_fields(table1_name);
            Set<String> fields2 = get_fields(table2_name);
            fields1.addAll(fields2);
            fields = (String[]) fields1.toArray(new String[0]);
            
            String onCondition = ((String)((JSONObject)queryJSON.get("on")).get("condition1")).split("\\.")[1].toString();
            String temp = ((String) queryJSON.get("joinType"));
            String joinType = temp.substring(temp.length() - 5);
            
            System.out.println(onCondition);

            
            Stream stream1 = sqlTopology.newStream(table1_name + "_spout", (FeederBatchSpout) spoutMap.get(table1_name));
            Stream stream2 = sqlTopology.newStream(table2_name + "_spout", (FeederBatchSpout) spoutMap.get(table2_name));
            
            Stream[] stream_array = {stream1,stream2};
            
//            sqlTopologyStream = sqlTopology.merge(new Fields(fields), stream_array);
            sqlTopologyStream = sqlTopology.join(stream1, new Fields(onCondition), stream2, new Fields(onCondition), new Fields(fields), JoinType.INNER);
//            System.out.println("join_complete");
            //Have to add different types of joins
        }
        else{ // Join not present
        	table_name = (String) queryJSON.get("table");
            if(table_name.equals("users")){
            	String[] tablefields = {"userid" , "age" , "gender" , "occupation" , "zipcode"};
            	fields = tablefields;
            }
            else if(table_name.equals("rating")){
            	String[] tablefields = {"userid" , "movieid" , "rating" , "timestamp"};
            	fields = tablefields;
            }
            else if(table_name.equals("movies")){
            	String[] tablefields = {"movieid" , "title" , "releasedate" , "unknown" , "Action" , "Adventure" , "Animation" ,
                "Children" , "Comedy" , "Crime" , "Documentary" , "Drama" , "Fantasy" ,
                "Film_Noir" , "Horror" , "Musical" , "Mystery" , "Romance" , "Sci_Fi" ,
                "Thriller" , "War" , "Western"};
            	fields = tablefields;
            }
            else if(table_name.equals("zipcodes")){
            	String[] tablefields = {"zipcode" , "zipcodetype" , "city" , "state"};
            	fields = tablefields;
            }
            sqlTopologyStream = sqlTopology.newStream(table_name + "_spout", (FeederBatchSpout) spoutMap.get(table_name));
        }

        //WHERE
//        System.out.println(fields.length);
        sqlTopologyStream = sqlTopologyStream.each(new Fields(fields), new WhereFilter());

        //GROUPBY
        if(queryJSON.has("groupByColumns")){
            String aggregateField = null;
        	JSONArray columns = (JSONArray) queryJSON.get("columns");
            JSONObject columnJSON = new JSONObject();
            for(int i=0; i<columns.length(); i++) {
            	if(columns.get(i) instanceof String) {
            		continue;
            	}
            	else {
            		columnJSON = (JSONObject) columns.get(i);
            		aggregateField = ((String) columnJSON.get("function")) + "(" + ((String) columnJSON.get("column")) + ")";
            	}
            }
            //HAVING
        	if(queryJSON.has("having")) {
        		String aggregateFcn = (String) ((JSONObject) queryJSON.get("having")).get("function");
        		if(aggregateFcn.equals("count")) {
                	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(aggregateField)).parallelismHint(16).newValuesStream();
                	List<String> having_fields = new ArrayList<String>();
                	for(String f : sqlTopologyStream.getOutputFields()) {
                		having_fields.add(f);
                	}
                	sqlTopologyStream = sqlTopologyStream.each(new Fields(having_fields), new HavingFilter());
        		}
        		else if(aggregateFcn.equals("min")) {
                	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), (CombinerAggregator) new Min(null), new Fields(aggregateField)).parallelismHint(6).newValuesStream();
                	List<String> having_fields = new ArrayList<String>();
                	for(String f : sqlTopologyStream.getOutputFields()) {
                		having_fields.add(f);
                	}
                	sqlTopologyStream = sqlTopologyStream.each(new Fields(having_fields), new HavingFilter());
        		}
        		else if(aggregateFcn.equals("max")) {
//        			System.out.println(aggregateField);
                	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).partitionAggregate(new Fields(fields),(Aggregator) new Max(aggregateFcn), new Fields(aggregateField)).toStream();
//                	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).toStream().maxBy((String) ((JSONObject) queryJSON.get("having")).get("column")).parallelismHint(6);
        			List<String> having_fields = new ArrayList<String>();
                	for(String f : sqlTopologyStream.getOutputFields()	) {
                		having_fields.add(f);
                	}
                	sqlTopologyStream = sqlTopologyStream.each(new Fields(having_fields), new HavingFilter());
        		}
                else if(aggregateFcn.equals("sum")) {
                	System.out.println((String)((JSONArray)queryJSON.get("groupByColumns")).get(0));
                	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields(aggregateField)).parallelismHint(16).newValuesStream();
                	List<String> having_fields = new ArrayList<String>();
                	for(String f : sqlTopologyStream.getOutputFields()) {
                		having_fields.add(f);
                		System.out.println(f);
                	}
                	sqlTopologyStream = sqlTopologyStream.each(new Fields(having_fields), new HavingFilter());            	
                }
                else {
                	System.out.println("Incorrect aggregator passed!");
                }
//        		sqlTopologyStream = sqlTopologyTridentState.newValuesStream();
        		
            } else {
//            	System.out.println(aggregateFcn);
            	sqlTopologyStream = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).toStream();
            }
        }
        //SELECT
        
        if(((JSONArray) queryJSON.get("columns")).get(0).equals("*")){
        	if(queryJSON.has("groupByColumns")) {
        		sqlTopologyStream = sqlTopologyStream.each(new Fields(fields), new SelectFilter(fields,(String)((JSONArray)queryJSON.get("groupByColumns")).get(0)), new Fields());
        	}
        	else {
        		sqlTopologyStream = sqlTopologyStream.each(new Fields(fields), new SelectFilter(fields,""), new Fields());
        	}
        } else{
//            String[] newFields;
            JSONArray columns = (JSONArray) queryJSON.get("columns");
            ArrayList<String> columnNames = new ArrayList<String>();
            JSONObject columnJSON = new JSONObject();
            for(int i=0; i<columns.length(); i++) {
            	if(columns.get(i) instanceof String) {
            		columnNames.add((String) columns.get(i));
            	}
            	else {
            		columnJSON = (JSONObject) columns.get(i);
            		columnNames.add((String) columnJSON.get("function") + "(" + (String) columnJSON.get("column") + ")");
            	}
            }
            if(queryJSON.has("groupByColumns")) {
            	sqlTopologyStream = sqlTopologyStream.each(new Fields((String[]) columnNames.toArray(new String[0])), new SelectFilter(columnNames,(String)((JSONArray)queryJSON.get("groupByColumns")).get(0)), new Fields());
            }
            else {
            	sqlTopologyStream = sqlTopologyStream.each(new Fields((String[]) columnNames.toArray(new String[0])), new SelectFilter(columnNames,""), new Fields());
            }
        }
        
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("trident", conf, sqlTopology.build());
        
        long start = new Date().getTime();
        
        if(queryJSON.has("joinType")) {
        	JSONObject table_json = (JSONObject) queryJSON.get("table");
        	String table1_name = (String) table_json.get("table1");
            String table2_name = (String) table_json.get("table2");
            spout_feeder(table1_name, (FeederBatchSpout) spoutMap.get(table1_name));
            spout_feeder(table2_name, (FeederBatchSpout) spoutMap.get(table2_name));
        }
        else {
        	spout_feeder(table_name, (FeederBatchSpout) spoutMap.get(table_name));
            }
        long end = new Date().getTime();
//        System.out.println("\n\n\n\n\n\n--------------------------------");
        System.out.println(end-start);
        
		FileWriter file = null;
		Map<String, Object> time_map = new HashMap<String, Object>(); 
		time_map.put("Storm Execution Time in ms", end-start);
		JSONObject map_json = new JSONObject(time_map);
        try {
            // Constructs a FileWriter given a file name, using the platform's default charset
        	file = new FileWriter("Storm_time.txt",false);
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
        cluster.shutdown();

        JSONObject stormResult = new JSONObject();
        JSONTokener jsonTokener = new JSONTokener(new FileReader(".\\Storm_output.json"));
        stormResult = new JSONObject(jsonTokener);

        File fp = new File(".\\Storm_output.json");

        // Creating an object of BufferedReader class
        BufferedReader br = new BufferedReader(new FileReader(fp));

        String st;
        String stormRes = null;
        while ((st = br.readLine()) != null)
            stormRes = st;

        stormJSON.put("Time", end-start);
        stormJSON.put("Storm Query Output", stormRes);
        return stormJSON;
    }

    public static void spout_feeder(String table_name, FeederBatchSpout fbs) {
		File file = new File(
				"data/input/" + table_name + ".csv"); // EDIT
		try {
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader(file));
			for (line = br.readLine(); line != null; line = br.readLine()) {
//				System.out.println(line);
				fbs.feed(ImmutableList.of(new Values(line.split(","))));
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    public static Set<String> get_fields(String table_name) {
    	Set<String> tablefields = new HashSet<String>();
    	if(table_name.equals("users")){
        	Collections.addAll(tablefields, "userid" , "age" , "gender" , "occupation" , "zipcode");
        }
        else if(table_name.equals("rating")){
        	Collections.addAll(tablefields, "userid" , "movieid" , "rating" , "timestamp");
        }
        else if(table_name.equals("movies")){
        	Collections.addAll(tablefields, "movieid" , "title" , "releasedate" , "unknown" , "Action" , "Adventure" , "Animation" ,
                    "Children" , "Comedy" , "Crime" , "Documentary" , "Drama" , "Fantasy" ,
                    "Film_Noir" , "Horror" , "Musical" , "Mystery" , "Romance" , "Sci_Fi" ,
                    "Thriller" , "War" , "Western");
        }
        else if(table_name.equals("zipcodes")){
        	Collections.addAll(tablefields, "zipcode" , "zipcodetype" , "city" , "state");
        }
        return tablefields;
    }
}
