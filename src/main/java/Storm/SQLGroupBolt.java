import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class SQLGroupBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        //READ MAPPING JSON
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
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        //tuple has 20 columns
        ArrayList<String> row;
        for(int i=0; i<tuple.size(); i++) {
            row.add(tuple.getString(i));
        }
        Table stormTable = stormToTable(Tuple tuple);

        //CALLING SQL FUNCTIONS
        Table result = SQLQueries.where(JSONObject queryJSON, Table data);

        Tuple result_tuple = tableToStorm(Table result);

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String table = (String) queryJSON.get("table");
//        JSONArray columnsArray = (JSONArray) queryJSON.get("columns");
//        ArrayList<String> columns;
//        for(int i=0; i<columnsArray; i++) {
//            columns.add(columnsArray.get(i));
//        }
        String[] fields = queryJSON.get(table).keySet();
        declarer.declare(new Fields(fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}