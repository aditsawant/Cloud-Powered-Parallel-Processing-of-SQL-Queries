import java.util.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.utils.DRPCClient;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.tuple.TridentTuple;

import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.BaseFilter;

import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.Split;
import storm.trident.testing.MemoryMapState;

import com.google.common.collect.ImmutableList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SQLExecutorTrident {
    public static void main(String[] args) throws Exception {
        //System.out.println("Log Analyser Trident");
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("c:\\file.json"));
            JSONObject mappingJSON =  (JSONObject) obj;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String[] fields = mappingJSON.get(table).keySet().toArray();

        TridentTopology sqlTopology = new TridentTopology();

//        FeederBatchSpout movieSpout = new FeederBatchSpout(ImmutableList.of("movieid" , "title" , "releasedate" , "unknown" , "Action" , "Adventure" , "Animation" ,
//                "Children" , "Comedy" , "Crime" , "Documentary" , "Drama" , "Fantasy" ,
//                "Film_Noir" , "Horror" , "Musical" , "Mystery" , "Romance" , "Sci_Fi" ,
//                "Thriller" , "War" , "Western"));
//        FeederBatchSpout userSpout = new FeederBatchSpout(ImmutableList.of("userid" , "age" , "gender" , "occupation" , "zipcode"));
//        FeederBatchSpout ratingSpout = new FeederBatchSpout(ImmutableList.of("userid" , "movieid" , "rating" , "timestamp"));
//        FeederBatchSpout zipcodeSpout = new FeederBatchSpout(ImmutableList.of("zipcode" , "zipcodetype" , "city" , "state"));

        UserSpout userSpout = new userSpout();
        MovieSpout userSpout = new movieSpout();
        RatingSpout userSpout = new ratingSpout();
        ZipcodeSpout userSpout = new zipcodeSpout();

        HashMap<String, Object> spoutMap = new HashMap<String, Object>();
        spoutMap.put("movies",movieSpout);
        spoutMap.put("users",userSpout);
        spoutMap.put("rating",ratingSpout);
        spoutMap.put("zipcodes",zipcodeSpout);


        String table = (String) queryJSON.get("table");
        String[] fields = new String[]();
        // Check if join is present
        if(queryJSON.opt("joinType")){
            String table1 = queryJSON.get(table).get(table1);
            String table2 = queryJSON.get(table).get(table2);
            Set<String> fields1 = new HashSet<Set> () { mappingJSON.get(table1).keySet() };
            Set<String> fields2 = new HashSet<Set> () { mappingJSON.get(table2).keySet() };
            fields = fields1.addAll(fields2).toArray();
            String onCondition = queryJSON.get(on).get(condition1).split(".")[1].trim();
            String temp = queryJSON.get("joinType");
            String joinType = temp[temp.length - 5] + temp[temp.length - 4] + temp[temp.length - 3] + temp[temp.length - 2] + temp[temp.length - 1];

            Stream stream1 = sqlTopology.newStream(table1 + "_spout", spoutMap[table1]);
            Stream stream2 = sqlTopology.newStream(table2 + "_spout", spoutMap[table2]);
            sqlTopology = sqlTopology.join(stream1, new Fields(onCondition), stream2, new Fields(onCondition), new Fields(fields), joinType.toUpperCase(), "COMPACT");
        }
        else{ // Join not present
            tablefields = mappingJSON.get(table).keySet().toArray();
            TridentTopology sqlTopology = topology.newStream(table + "_spout", spoutMap[table])
        }

        //WHERE
        sqlTopology = sqlTopology.each(new Fields(fields), new WhereFilter());

        if(queryJSON.opt("groupByColumns")){

            if(queryJSON.opt("having")) {
                switch(queryJSON.get("having").get("function")){
                    case "count":{
                        sqlTopology = sqlTopology.groupby(new Fields(queryJSON.get("groupByColumns")[0])).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
                    }
                    case "min":{
                        sqlTopology = sqlTopology.groupby(new Fields(queryJSON.get("groupByColumns")[0])).persistentAggregate(new MemoryMapState.Factory(), new Min(), new Fields("min"));
                    }
                    case "max":{
                        sqlTopology = sqlTopology.groupby(new Fields(queryJSON.get("groupByColumns")[0])).persistentAggregate(new MemoryMapState.Factory(), new Max(), new Fields("max"));
                    }
                    case: "sum":{
                        sqlTopology = sqlTopology.groupby(new Fields(queryJSON.get("groupByColumns")[0])).persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields("sum"));
                    }
                }
            } else {
                sqlTopology = sqlTopology.groupby(new Fields(queryJSON.get("groupByColumns")[0]));
            }
        }
        if(queryJSON.get("columns")[0].equals("*")){
            sqlTopology = sqlTopology.each(new Fields(fields), new SelectStatement(), new Fields());
        } else{
            String[] newFields;
            for(column : queryJson.columns){
                if(column.opt("function")){
                    newFields.add(column.get("function"));
                }else{
                    newFields.add(column);
                }
            }
            sqlTopology = sqlTopology.each(new Fields(newFields), new SelectStatement(), new Fields());
        }

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident", conf, sqlTopology.build());

//        LocalDRPC drpc = new LocalDRPC();
//
//        topology.newDRPCStream("call_count", drpc)
//                .stateQuery(callCounts, new Fields("args"), new MapGet(), new Fields("count"));
//
//        topology.newDRPCStream("multiple_call_count", drpc)
//                .each(new Fields("args"), new CSVSplit(), new Fields("call"))
//                .groupBy(new Fields("call"))
//                .stateQuery(callCounts, new Fields("call"), new MapGet(),
//                        new Fields("count"))
//                .each(new Fields("call", "count"), new Debug())
//                .each(new Fields("count"), new FilterNull())
//                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
//
//        Config conf = new Config();
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("trident", conf, topology.build());
//        Random randomGenerator = new Random();
//        int idx = 0;
//
//        while(idx < 10) {
//            testSpout.feed(ImmutableList.of(new Values("1234123401",
//                    "1234123402", randomGenerator.nextInt(60))));
//
//            testSpout.feed(ImmutableList.of(new Values("1234123401",
//                    "1234123403", randomGenerator.nextInt(60))));
//
//            testSpout.feed(ImmutableList.of(new Values("1234123401",
//                    "1234123404", randomGenerator.nextInt(60))));
//
//            testSpout.feed(ImmutableList.of(new Values("1234123402",
//                    "1234123403", randomGenerator.nextInt(60))));
//
//            idx = idx + 1;
//        }
//
//        System.out.println("DRPC : Query starts");
//        System.out.println(drpc.execute("call_count","1234123401 - 1234123402"));
//        System.out.println(drpc.execute("multiple_call_count", "1234123401 -
//                1234123402,1234123401 - 1234123403"));
//                System.out.println("DRPC : Query ends");
//
//        cluster.shutdown();
//        drpc.shutdown();

        // DRPCClient client = new DRPCClient("drpc.server.location", 3772);
    }
}

public class WhereFilter extends BaseFilter {
    public boolean isKeep(TridentTuple tuple) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("c:\\file.json"));
            JSONObject mappingJSON =  (JSONObject) obj;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String value1 = (String) queryJSON.get("where").get("value1");
        String value2 = (String) queryJSON.get("where").get("value2");
        int field_index = tuple.fieldIndex(value1);
        String operator = (String) queryJSON.get("where").get("operator");

        switch(operator) {
            case "=": {
                return tuple.getString(fieldIndex).equalsIgnoreCase(value2);
            }
            case ">": {
                return Integer.parseInt(tuple.getString(fieldIndex)) > Integer.parseInt(value2);
            }
            case "<": {
                return Integer.parseInt(tuple.getString(fieldIndex)) < Integer.parseInt(value2);
            }
            case ">=": {
                return Integer.parseInt(tuple.getString(fieldIndex)) >= Integer.parseInt(value2);
            }
            case "<=": {
                return Integer.parseInt(tuple.getString(fieldIndex)) <= Integer.parseInt(value2);
            }
            case "<>": {
                return Integer.parseInt(tuple.getString(fieldIndex)) != Integer.parseInt(value2);
            }
            case "like": {
                return tuple.getString(fieldIndex).contains(value2);
            }
//            case "in": {
//
//            }

        }
    }
}