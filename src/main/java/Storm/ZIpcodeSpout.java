import java.util.*;
//import storm tuple packages
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

//import Spout interface packages
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.atomic.AtomicInteger;

//Create a class FakeLogReaderSpout which implement IRichSpout interface to access functionalities

public class Zipcodepout implements IRichSpout {
    //Create instance for SpoutOutputCollector which passes tuples to bolt.
    private SpoutOutputCollector collector;
    AtomicInteger cnt = new AtomicInteger(0);
    //Create instance for TopologyContext which contains topology data.
    private TopologyContext context;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.cnt = cnt;
        this.query = queryJSON;
    }

    @Override
    public void nextTuple() {
        // get path to table file from json query
        //Read csv file data
        Utils.sleep(1000);
        File file = new File(<path_to_csv>);  // EDIT ME TO YOUR PATH!
        String[] observation = null;
        int i = 0;
        try {
            String line = "";
            BufferedReader br = new BufferedReader(new FileReader(file));
            while (i++ <= this.cnt.get()) {
                line = br.readLine(); // stream through to next line
            }
            observation = line.split(",");
        } catch (Exception e) {
            e.printStackTrace();
            this.cnt.set(0);
        }
        this.cnt.getAndIncrement();
        this.collector.emit(new Values(observation));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("ColumnIndexMapping.json));
                    JSONObject mappingJSON =  (JSONObject) obj;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String[] fields = mappingJSON.get(table).keySet().toArray();
//        -----------fields_list = <list of cols of the table>--------------------
//        String[] fields = fields_list.toArray(new String[fields_list.size()]); // emit these fields
        declarer.declare(new Fields(fields));
    }

    //Override all the interface methods
    @Override
    public void close() {}

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}