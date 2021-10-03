package demo.trident_demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.trident.JoinType;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.tuple.TridentTuple;

import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Max;
import org.apache.storm.trident.operation.builtin.Min;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.testing.MemoryMapState;

import com.google.common.collect.ImmutableList;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import demo.trident_demo.UserSpout;
import demo.trident_demo.ZipcodeSpout;
import demo.trident_demo.RatingSpout;
import demo.trident_demo.MovieSpout;

import demo.trident_demo.WhereFilter;
import demo.trident_demo.SelectFilter;


public class SQLExecutorTrident {
    public static void main(String[] args) throws Exception {
        //System.out.println("Log Analyser Trident");
        JSONParser parser = new JSONParser();
        JSONObject mappingJSON = new JSONObject();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader("C:\\Users\\Maithil\\Desktop\\eclipse\\workspace\\trident_demo\\src\\main\\java\\demo\\trident_demo\\ColumnIndexMapping.json"));
            mappingJSON = new JSONObject(jsonTokener);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        JSONParser parser2 = new JSONParser();
        JSONObject queryJSON = new JSONObject();
        try {
            JSONTokener jsonTokener = new JSONTokener(new FileReader("C:\\Users\\Maithil\\Desktop\\eclipse\\workspace\\trident_demo\\src\\main\\java\\demo\\trident_demo\\query.json"));
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
//            for(String s : fields1) {
//            	System.out.println(s);
//            }
//            int m = ((String)((JSONObject)queryJSON.get("on")).get("condition1")).split(".").length;
//            System.out.println(m);
            
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
                	sqlTopologyTridentState = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(aggregateField)).parallelismHint(6);       
                	sqlTopologyStream = sqlTopologyTridentState.newValuesStream();
                	List<String> having_fields = new ArrayList<String>();
                	for(String f : sqlTopologyStream.getOutputFields()) {
                		having_fields.add(f);
                	}
                	System.out.println("Reached here.");
                	sqlTopologyStream = sqlTopologyStream.each(new Fields(having_fields), new MyFilter());
        		}
        		else if(aggregateFcn.equals("min")) {
                	sqlTopologyTridentState = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), (CombinerAggregator) new Min(null), new Fields("min"));
        		}
        		else if(aggregateFcn.equals("max")) {
                	sqlTopologyTridentState = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), (CombinerAggregator) new Max(null), new Fields("max"));
        		}
                else if(aggregateFcn.equals("sum")) {
                	sqlTopologyTridentState = sqlTopologyStream.groupBy(new Fields((String)((JSONArray)queryJSON.get("groupByColumns")).get(0))).persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields("sum"));
            	}
                else {
                	System.out.println("Incorrect aggregator passed!");
                }
        		sqlTopologyStream = sqlTopologyTridentState.newValuesStream();
        		
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
        
        cluster.shutdown();
        }    

    
    public static void spout_feeder(String table_name, FeederBatchSpout fbs) {
		File file = new File(
				"C:\\Users\\Maithil\\Desktop\\eclipse\\workspace\\trident_demo\\src\\main\\java\\demo\\trident_demo\\" + table_name + ".csv"); // EDIT
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
