package Hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.*;
//import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.json.*;
import Utils.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import scala.collection.mutable.StringBuilder;

public class HadoopExecutor {
    public static String[] jsonArrayToStringArray(JSONArray jsonArray) {
        int arraySize = jsonArray.length();
        String[] stringArray;
        ArrayList<String> bufStringArray = new ArrayList<String>();
        for (int i = 0; i < arraySize; i++) {
            if (jsonArray.get(i).toString().charAt(0) == '{')
                continue;

            bufStringArray.add(jsonArray.get(i).toString());
        }
        stringArray = new String[bufStringArray.size()];
        stringArray = bufStringArray.toArray(stringArray);
        return stringArray;
    }

    //  static final Logger log = Logger.getLogger(HadoopExecutor.class.getName());
    public static class MultiQueryMapper1
            extends Mapper<LongWritable, Text, Text, Text> {
        private static JSONObject queryJSON;
        private String FILETAG = "F1,";

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            setQueryJSON(new JSONObject(conf.get("queryJSONString")));
            System.out.println("Inside mapper Setup ========> ");
            //System.out.println(queryJSON.toString());
        }

        private Text outputRow = new Text();

        public static JSONObject getQueryJSON() {
            return queryJSON;
        }

        public void setOutput(Context context, Table row, Text value) throws IOException, InterruptedException {

            String outputKey = FILETAG;
            String table1ColumnName = ((JSONObject) queryJSON.get("on")).getString("condition1");
            table1ColumnName = table1ColumnName.substring(queryJSON.getJSONObject("table").getString("table1").length() + 1);
            outputKey = (String) row.getColumnValue(table1ColumnName).toString();
            outputRow.set(outputKey);
            //System.out.println(outputRow.toString());

            //value=(new Text()).set(FILETAG)
            Text outputValue = new Text();
            outputValue.set(FILETAG + value.toString());
            context.write(outputRow, outputValue);
        }

        public static void setQueryJSON(JSONObject queryJSON) {
            MultiQueryMapper1.queryJSON = queryJSON;
        }

        public void Where(String operator, JSONObject whereJSON, Table row, Context context, Text value) throws IOException, InterruptedException {
            switch (operator) {
                case "=": {
                    //System.out.println(row.getColumnValue(queryJSON.toString()));
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();

                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }
                    if (queryColumnValue.equalsIgnoreCase(columnValue)) {
                        System.out.println("inside =");
                        setOutput(context, row, value);
                    }
                    break;
                }
                case "<": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) < 0) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }
                case ">": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) > 0) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }
                case "<=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) <= 0) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }
                case ">=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) >= 0) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }
                case "in": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String columnSetString = (String) whereJSON.get("value2");
                    String[] columnSetArray = columnSetString.split(",");
                    for (String s : columnSetArray) {
                        val2Array.add(s.trim());
                    }
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    if (val2Array.contains(columnValue)) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }
                case "like": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String patternString = (String) whereJSON.get("value2");
                    patternString = patternString.substring(1, patternString.length() - 1);
                    Pattern pattern = Pattern.compile(patternString);
                    System.out.println(patternString);
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    Matcher matcher = pattern.matcher(columnValue);
                    System.out.println(patternString + " " + columnValue);
                    if (matcher.matches()) {
                        setOutput(context, row, value);
//
                    }
                    break;
                }

            }
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String tableName = queryJSON.getJSONObject("table").getString("table1");
            Table row = TableFactory.getTable(tableName, value.toString());

            JSONObject whereJSON = queryJSON.getJSONObject("where");
			/*String []outputColumns;
			JSONArray columnArray=queryJSON.getJSONArray("columns");
			if(columnArray.get(0).equals("*"))
			{
				outputColumns=((User)row).getColumnList();
			}
			else
			{
				ArrayList<String> tempArray = new ArrayList<>();
				for(Object x: columnArray)
				{
					String temp=(String)x;
					if(temp.length()>tableName.length())
					{
						if(tableName.equals(temp.substring(0,tableName.length())))
						{
							tempArray.add(temp.substring(tableName.length()));
						}
					}
				}
				outputColumns=(String [])tempArray.toArray();
			}*/
            setOutput(context, row, value);
        }
    }

    public static class MultiQueryMapper2
            extends Mapper<LongWritable, Text, Text, Text> {
        private static JSONObject queryJSON;
        private String FILETAG = "F2,";

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            setQueryJSON(new JSONObject(conf.get("queryJSONString")));
            System.out.println("Inside mapper2 Setup ========> ");
            System.out.println(conf.get("queryJSONString"));
        }

        private Text outputRow = new Text();

        public static JSONObject getQueryJSON() {
            return queryJSON;
        }

        public void setOutput(Context context, Table row, Text value) throws IOException, InterruptedException {

            String outputKey = FILETAG;
            String table2ColumnName = ((JSONObject) queryJSON.get("on")).getString("condition2");

            table2ColumnName = table2ColumnName.substring(queryJSON.getJSONObject("table").getString("table2").length() + 1);
            //System.out.println(table2ColumnName);
            outputKey = (String) row.getColumnValue(table2ColumnName).toString();
            outputRow.set(outputKey);


            //value=(new Text()).set(FILETAG)
            Text outputValue = new Text();
            outputValue.set(FILETAG + value.toString());
            context.write(outputRow, outputValue);
        }

        public static void setQueryJSON(JSONObject queryJSON) {
            MultiQueryMapper2.queryJSON = queryJSON;
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
//			if(key.get() == 0) {
//				return;
//			}
            String tableName = queryJSON.getJSONObject("table").getString("table2");
            Table row = TableFactory.getTable(tableName, value.toString());
            if (row == null)
                return;

            setOutput(context, row, value);
        }
    }

    public static class QueryMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private static JSONObject queryJSON;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            setQueryJSON(new JSONObject(conf.get("queryJSONString")));
            System.out.println("Inside Reducer Setup ========> ");
            //System.out.println(queryJSON.toString());
        }

        private Text outputRow = new Text();

        public void setOutput(Context context, Text outputRow, String[] outputColumns, Table row, Text value) throws IOException, InterruptedException {
            if (queryJSON.keySet().contains((String) "groupByColumns")) {

                outputRow.set(row.groupByString(jsonArrayToStringArray(queryJSON.getJSONArray("groupByColumns"))));
                context.write(outputRow, value);
                return;
            }
            String outputValue = "";
            for (String column : outputColumns) {
                outputValue = outputValue + row.getColumnValue(column) + " ";
            }
            outputValue.trim();
            System.out.println(outputValue);
            outputRow.set(outputValue);
            //System.out.println(outputRow.toString());
            context.write(outputRow, value);
        }

        public void Where(String operator, JSONObject whereJSON, Table row, String[] outputColumns, Context context, Text value) throws IOException, InterruptedException {
            switch (operator) {
                case "=": {
                    //System.out.println(row.getColumnValue(queryJSON.toString()));
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.equalsIgnoreCase(queryColumnValue)) {
                        setOutput(context, outputRow, outputColumns, row, value);
                    }
                    break;
                }
                case "<": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) < 0) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }
                case ">": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) > 0) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }
                case "<=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) <= 0) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }
                case ">=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) >= 0) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }
                case "in": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String columnSetString = (String) whereJSON.get("value2");
                    String[] columnSetArray = columnSetString.split(",");
                    for (String s : columnSetArray) {
                        val2Array.add(s.trim());
                    }
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    if (val2Array.contains(columnValue)) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }
                case "like": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String patternString = (String) whereJSON.get("value2");
                    patternString = patternString.substring(1, patternString.length() - 1);
                    Pattern pattern = Pattern.compile(patternString);
                    System.out.println(patternString);
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1"))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    Matcher matcher = pattern.matcher(columnValue);
                    System.out.println(patternString + " " + columnValue);
                    if (matcher.matches()) {
                        setOutput(context, outputRow, outputColumns, row, value);
//
                    }
                    break;
                }

            }
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
//			if(key.get() == 0) {
//				return;
//			}
            String tableName = queryJSON.getString("table");

            Table row = TableFactory.getTable(tableName, value.toString());
            if (row == null)
                return;

            JSONObject whereJSON = queryJSON.getJSONObject("where");
            whereJSON = (JSONObject) queryJSON.get("where");
            String[] outputColumns;
            JSONArray columnArray = queryJSON.getJSONArray("columns");
            if (columnArray.get(0).equals("*")) {
                outputColumns = (row).getColumnList();
            } else {
                outputColumns = jsonArrayToStringArray(columnArray);
            }

            //System.out.println(row.getColumnValue(whereJSON.get("value1").toString())+" "+whereJSON.getString("value2").toString());
            //System.out.println(row.checkColumnValue(whereJSON.get("value1").toString(),whereJSON.get("value2").toString()));
            //System.out.println(whereJSON.getString("value2").toString());
            String operator = whereJSON.getString("operator");
            Where(operator, whereJSON, row, outputColumns, context, value);
			/*switch(operator)
			{
				case "=":
				{
					//System.out.println(row.getColumnValue(queryJSON.toString()));
					String queryColumnValue='\"'+whereJSON.getString("value2")+'\"';

					if(row.checkColumnValue(whereJSON.getString("value1"),queryColumnValue))
					{
						String outputValue="";
						for(String column : outputColumns)
						{
							outputValue = outputValue + row.getColumnValue(column)+" ";
						}
							outputValue.trim();
							outputRow.set(outputValue);
							//System.out.println(outputRow.toString());
							context.write(outputRow, value);
					}
					break;
				}
				case "<":
				{
					String queryColumnValue=whereJSON.getString("value2");
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);
						//queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
					}

					if(columnValue.compareToIgnoreCase(queryColumnValue)<0)
					{
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}
				case ">":
				{
					String queryColumnValue=whereJSON.getString("value2");
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);
						//queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
					}

					if(columnValue.compareToIgnoreCase(queryColumnValue)>0)
					{
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}
				case "<=":
				{
					String queryColumnValue=whereJSON.getString("value2");
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);
						//queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
					}

					if(columnValue.compareToIgnoreCase(queryColumnValue)<=0)
					{
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}
				case ">=":
				{
					String queryColumnValue=whereJSON.getString("value2");
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);
						//queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
					}

					if(columnValue.compareToIgnoreCase(queryColumnValue)>=0)
					{
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}
				case "in":
				{
					//System.out.println("inside IN");
					ArrayList<String> val2Array = new ArrayList<>();
					//val2Array=(ArrayList<String>) whereJSON.get("value2");
					String columnSetString=(String)whereJSON.get("value2");
					String [] columnSetArray=columnSetString.split(",");
					for(String s : columnSetArray)
					{
						val2Array.add(s.trim());
					}
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);

					}
					if(val2Array.contains(columnValue)){
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}
				case "like":
				{
					//System.out.println("inside IN");
					ArrayList<String> val2Array = new ArrayList<>();
					//val2Array=(ArrayList<String>) whereJSON.get("value2");
					String patternString=(String)whereJSON.get("value2");
					patternString=patternString.substring(1,patternString.length()-1);
					Pattern pattern= Pattern.compile(patternString);
					System.out.println(patternString);
					String columnValue=(String)(row.getColumnValue(whereJSON.getString("value1"))).toString();
					if(columnValue.charAt(0)=='\"')
					{
						columnValue=columnValue.substring(1,columnValue.length()-1);

					}
					Matcher matcher=pattern.matcher(columnValue);
					System.out.println(patternString+" "+columnValue);
					if(matcher.matches()){
						setOutput(context,outputRow,outputColumns,row,value);
//
					}
					break;
				}

			}*/
//
        }

        public static JSONObject getQueryJSON() {
            return queryJSON;
        }

        public static void setQueryJSON(JSONObject queryJSON) {
            QueryMapper.queryJSON = queryJSON;
        }
    }

    public static class QueryReducer
            extends Reducer<Text, Text, Text, Text> {
        private static JSONObject queryJSON;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
//			System.out.println("Inside Reducer Setup ========> ");
//			System.out.println(conf.get("queryJSONString"));
            setQueryJSON(new JSONObject(conf.get("queryJSONString")));
//	        System.out.println("Inside reducer Setup =====> ".concat(queryJSON.toString()));
        }

        private Text outputRowText = new Text();

        public boolean whereReduce(JSONObject whereJSON, Table row, String tableName) {
            String operator = whereJSON.getString("operator");

            switch (operator) {
                case "=": {
                    //System.out.println(row.getColumnValue(queryJSON.toString()));
                    String queryColumnValue = whereJSON.getString("value2");

                    String columnValue = row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1)).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }
                    if (columnValue.equalsIgnoreCase(queryColumnValue)) {
                        return true;
                    }
                    break;
                }
                case "<": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) < 0) {
                        return true;
//
                    }
                    break;
                }
                case ">": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) > 0) {
                        return true;
//
                    }
                    break;
                }
                case "<=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) <= 0) {
                        return true;
//
                    }
                    break;
                }
                case ">=": {
                    String queryColumnValue = whereJSON.getString("value2");
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);
                        //queryColumnValue='\"'+whereJSON.getString("value2")+'\"';
                    }

                    if (columnValue.compareToIgnoreCase(queryColumnValue) >= 0) {
                        return true;
//
                    }
                    break;
                }
                case "in": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String columnSetString = (String) whereJSON.get("value2");
                    String[] columnSetArray = columnSetString.split(",");
                    for (String s : columnSetArray) {
                        val2Array.add(s.trim());
                    }
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    if (val2Array.contains(columnValue)) {
                        return true;
//
                    }
                    break;
                }
                case "like": {
                    //System.out.println("inside IN");
                    ArrayList<String> val2Array = new ArrayList<>();
                    //val2Array=(ArrayList<String>) whereJSON.get("value2");
                    String patternString = (String) whereJSON.get("value2");
                    patternString = patternString.substring(1, patternString.length() - 1);
                    Pattern pattern = Pattern.compile(patternString);
                    System.out.println(patternString);
                    String columnValue = (String) (row.getColumnValue(whereJSON.getString("value1").substring(tableName.length() + 1))).toString();
                    if (columnValue.charAt(0) == '\"') {
                        columnValue = columnValue.substring(1, columnValue.length() - 1);

                    }
                    Matcher matcher = pattern.matcher(columnValue);
                    System.out.println(patternString + " " + columnValue);
                    if (matcher.matches()) {
                        return true;
                    }
                    break;
                }
            }
            return false;
        }

        void ExecuteJoinReduce(Text key, Iterable<Text> values,
                               Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            ArrayList<Table> f1Values = new ArrayList<>();
            ArrayList<Table> f2Values = new ArrayList<>();
            String table1Name = queryJSON.getJSONObject("table").getString("table1");
            String table2Name = queryJSON.getJSONObject("table").getString("table2");

            JSONObject whereJSON = queryJSON.getJSONObject("where");


            int table = 0;
            if (whereJSON.getString("value1").length() > table2Name.length() && whereJSON.getString("value1").substring(0, table2Name.length()).equals(table2Name))
                table = 1;

            for (Text textValues : values) {
                String[] valueArray = textValues.toString().split(",");

                if (valueArray[0].equals("F1")) {
                    f1Values.add(TableFactory.getTable(table1Name, textValues.toString().substring(3)));
                } else {
                    f2Values.add(TableFactory.getTable(table2Name, textValues.toString().substring(3)));
                }
            }
            int f1Length = f1Values.size();
            int f2Length = f2Values.size();

            ArrayList<String> T1Columns = new ArrayList<>();
            ArrayList<String> T2Columns = new ArrayList<>();
            JSONArray columnArray = queryJSON.getJSONArray("columns");

            String[] outputColumns = jsonArrayToStringArray(columnArray);

            if (outputColumns[0].equals("*")) {
                if (f1Length > 0) {
                    T1Columns = new ArrayList<String>(Arrays.asList(f1Values.get(0).getColumnList()));
                } else {
                    switch (table1Name) {
                        case "users": {
                            T1Columns = new ArrayList<String>(Arrays.asList((new User()).getColumnList()));
                            break;
                        }
                        case "movies":
                            T1Columns = new ArrayList<String>(Arrays.asList((new Movie()).getColumnList()));
                            break;
                        case "rating":
                            T1Columns = new ArrayList<String>(Arrays.asList((new Rating()).getColumnList()));
                            break;
                        case "zipcode":
                            T1Columns = new ArrayList<String>(Arrays.asList((new Zipcode()).getColumnList()));
                            break;
                    }
                }
                if (f2Length > 0) {
                    T2Columns = new ArrayList<String>(Arrays.asList(f2Values.get(0).getColumnList()));
                } else {
                    switch (table2Name) {
                        case "users": {
                            T2Columns = new ArrayList<String>(Arrays.asList((new User()).getColumnList()));
                            break;
                        }
                        case "movies":
                            T2Columns = new ArrayList<String>(Arrays.asList((new Movie()).getColumnList()));
                            break;
                        case "rating":
                            T2Columns = new ArrayList<String>(Arrays.asList((new Rating()).getColumnList()));
                            break;
                        case "zipcode":
                            T2Columns = new ArrayList<String>(Arrays.asList((new Zipcode()).getColumnList()));
                            break;
                    }
                }
            } else {
                for (String column : outputColumns) {
                    if (column.length() > table1Name.length()) {
                        if (column.substring(0, table1Name.length()).equals(table1Name)) {
                            T1Columns.add(column.substring(table1Name.length() + 1, column.length()));
                        }
                    }
                    if (column.length() > table2Name.length()) {
                        if (queryJSON.getString("joinType").equalsIgnoreCase("natural") && queryJSON.getJSONObject("on").getString("condition2").substring(table2Name.length() + 1).equals(column))
                            continue;
                        if (column.substring(0, table2Name.length()).equals(table2Name)) {
                            T2Columns.add(column.substring(table2Name.length() + 1, column.length()));
                        }
                    }
                }
            }


            if (queryJSON.getString("joinType").equals("left")) {
                if (f2Length == 0) {
                    switch (table2Name) {
                        case "users": {
                            f2Values.add(new User());
                            break;
                        }
                        case "movies":
                            f2Values.add(new Movie());
                            break;
                        case "rating":
                            f2Values.add(new Rating());
                            break;
                        case "zipcode":
                            f2Values.add(new Zipcode());
                            break;
                    }
                }
            } else if (queryJSON.getString("joinType").equals("right")) {
                if (f1Length == 0) {
                    switch (table1Name) {
                        case "users": {
                            f1Values.add(new User());
                            break;
                        }
                        case "movies":
                            f1Values.add(new Movie());
                            break;
                        case "rating":
                            f1Values.add(new Rating());
                            break;
                        case "zipcode":
                            f1Values.add(new Zipcode());
                            break;
                    }
                }
            } else if (queryJSON.getString("joinType").equals("outer")) {

                if (f1Length == 0) {
                    switch (table1Name) {
                        case "users": {
                            f1Values.add(new User());
                            break;
                        }
                        case "movies":
                            f1Values.add(new Movie());
                            break;
                        case "rating":
                            f1Values.add(new Rating());
                            break;
                        case "zipcode":
                            f1Values.add(new Zipcode());
                            break;
                    }
                } else if (f2Length == 0) {
                    switch (table2Name) {
                        case "users": {

                            f2Values.add(new User());
                            break;
                        }
                        case "movies":
                            f2Values.add(new Movie());
                            break;
                        case "rating":
                            f2Values.add(new Rating());
                            break;
                        case "zipcode":
                            f2Values.add(new Zipcode());
                            break;
                    }
                }
            }
            for (Table f1Row : f1Values) {
                StringBuilder outputString = new StringBuilder();

                if (f1Row == null)
                    continue;
                if (table == 0) {
                    if (!whereReduce(whereJSON, f1Row, table1Name))
                        continue;
                }
                for (String s : T1Columns) {
                    outputString = outputString.append(" " + f1Row.getColumnValue(s));
                }

                for (Table f2Row : f2Values) {
                    StringBuilder outputString2 = new StringBuilder();
                    if (f2Row == null)
                        continue;
                    if (table == 1) {
                        if (!whereReduce(whereJSON, f2Row, table2Name))
                            continue;
                    }
                    for (String s : T2Columns) {
                        outputString2 = outputString2.append(" " + f2Row.getColumnValue(s));
                    }
                    StringBuilder temp = new StringBuilder(outputString.toString());

                    temp.append(outputString2);
                    outputRowText.set(temp.toString());
                    Text tx = new Text();
                    context.write(outputRowText, tx);
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //System.out.println(key.toString());
            //System.out.println(values);

            if (!queryJSON.keySet().contains((String) "groupByColumns") && !queryJSON.keySet().contains((String) "joinType")) {
                Text tx = new Text();
                context.write(key, tx);
//			for( Text t : values)
//			{
//
//			}

                return;
            }
            if (queryJSON.keySet().contains((String) "joinType")) {

                ExecuteJoinReduce(key, values, context);
                return;
            }
            String tableName = queryJSON.getString("table");
            ArrayList<Table> arr = new ArrayList<Table>();
            for (Text v : values) {
//    		System.out.println("value in reducer =====> ");
//    		System.out.println(v.toString());
                Table row = TableFactory.getTable(tableName, v.toString());
//    		System.out.println("inside reducer === ");
                //System.out.println(tableName);
                if (row == null)
                    continue;
                arr.add(row);
            }

            if (arr.size() == 0)
                return;
            JSONObject havingJSON = queryJSON.getJSONObject("having");
            String function = havingJSON.getString("function");
            JSONArray columnsArray = queryJSON.getJSONArray("columns");
            String outputRow = new String("");

////    	System.out.println("arr =====> ");
////    	System.out.println(arr.get(0));
////    	System.out.println(havingJSON);
            String aggr = arr.get(0).getAggregate(function, havingJSON.getString("column"), arr).toString();
            if (arr.get(0).compareAggregate(havingJSON.getString("column"), havingJSON.getString("function"),
                    havingJSON.getString("operator"), havingJSON.getString("value"), arr) == false) {
                //System.out.println("aggregate not matched================");
                return;
            }
            //System.out.println(aggr);
            for (int i = 0; i < columnsArray.length(); i++) {
//			System.out.println("columns array ===>");
//			System.out.println(arr.get(0));

//			JSONObject temp = new JSONObject(columnsArray.get(i).toString());
//
                if (columnsArray.get(i).toString().charAt(0) == '{')
                    continue;
                System.out.println(columnsArray.get(i));
                outputRow = outputRow.concat(arr.get(0).getColumnValue(columnsArray.getString(i)).toString()).concat(",");
            }
            outputRow = outputRow.concat(aggr);
//		System.out.println("printing oputput row === ");
//		System.out.println(outputRow);
            outputRowText.set(outputRow);
            Text tx = new Text();
            context.write(tx, outputRowText);
        }

        public static JSONObject getQueryJSON() {
            return queryJSON;
        }

        public static void setQueryJSON(JSONObject queryJSON) {
            QueryReducer.queryJSON = queryJSON;
        }
    }

    public static JSONObject parseSQL(String query) {

        //ignore semicolon at the end;
        if (query.charAt(query.length() - 1) == ';') {
            query = query.substring(0, query.length() - 1);
        }
        //remove all the inverted commas
        query = query.replaceAll("[']", " ");

        Pattern pattern1 = Pattern.compile("select (.+(,.+)*) from(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+)");
        Pattern pattern2 = Pattern.compile("select (.+(,.+)*) from(.+) ((natural)? ((left|right) outer )|inner )?join (.+) on (.+)=(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+)");
        Pattern pattern3 = Pattern.compile("select (.+(,.+)*) from(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+) group by (.+) having (sum|count|max|min)\\((.+)\\) (>=|<=|==|!=|>|<) (.+)");
        Matcher matcher1 = pattern1.matcher(query);
        Matcher matcher2 = pattern2.matcher(query);
        Matcher matcher3 = pattern3.matcher(query);
        JSONObject queryJSON = new JSONObject();

        if (matcher2.matches()) {
            String[] columns = matcher2.group(1).trim().split(",");
            JSONArray columnsList = new JSONArray();
            for (String column : columns) {
                columnsList.put(column.trim());
            }
            queryJSON.put("columns", columnsList);

//		  JOIN part
            String joinType = query.split("join")[0].split("from")[1];
            String[] arr = joinType.split(" ", 3);
            if (arr[2].equals("")) {
                arr[2] = "inner";
            }
            queryJSON.put("joinType", arr[2].split(" ", 2)[0]);

            JSONObject tableJSON = new JSONObject();
            tableJSON.put("table1", matcher2.group(3).split(" ")[1].trim());
            tableJSON.put("table2", matcher2.group(8).trim());
            queryJSON.put("table", tableJSON);

            JSONObject conditionJSON = new JSONObject();
            conditionJSON.put("condition1", matcher2.group(9).trim());
            conditionJSON.put("condition2", matcher2.group(10).trim());
            queryJSON.put("on", conditionJSON);

//		  WHERE part
            JSONObject whereJSON = new JSONObject();
            whereJSON.put("value1", matcher2.group(11).trim());
            whereJSON.put("operator", matcher2.group(12).trim());
            whereJSON.put("value2", matcher2.group(13).trim());
            queryJSON.put("where", whereJSON);
//		  log.info(queryJSON);
        } else if (matcher3.matches()) {
            //SELECT
            String[] columns = matcher3.group(1).trim().split(",");
            JSONArray columnsList = new JSONArray();
            for (String column : columns) {
                if (column.trim().contains("(")) {
                    JSONObject aggregateJSON = new JSONObject();
                    aggregateJSON.put("function", column.split("\\(")[0].trim());
                    String temp = column.split("\\(")[1].trim();
                    temp = temp.substring(0, temp.length() - 1);
                    aggregateJSON.put("column", temp);
                    columnsList.put(aggregateJSON);
                } else {
                    columnsList.put(column.trim());
                }
            }
            queryJSON.put("columns", columnsList);

            //FROM
            queryJSON.put("table", matcher3.group(3).trim());

            //WHERE
            JSONObject whereJSON = new JSONObject();
            whereJSON.put("value1", matcher3.group(4).trim());
            whereJSON.put("operator", matcher3.group(5).trim());
            whereJSON.put("value2", matcher3.group(6).trim());
            queryJSON.put("where", whereJSON);

            //GROUP BY
            String[] groupByColumns = matcher3.group(7).trim().split(",");
            JSONArray groupByColumnsList = new JSONArray();
            for (String groupByColumn : groupByColumns) {
                groupByColumnsList.put(groupByColumn.trim());
            }
            queryJSON.put("groupByColumns", groupByColumnsList);

            //HAVING
            JSONObject havingJSON = new JSONObject();
            havingJSON.put("function", matcher3.group(8).trim());
            havingJSON.put("column", matcher3.group(9).trim());
            havingJSON.put("operator", matcher3.group(10).trim());
            havingJSON.put("value", matcher3.group(11).trim());
            queryJSON.put("having", havingJSON);
            //log.info(queryJSON);
        } else if (matcher1.matches()) {
            //SELECT
            String[] columns = matcher1.group(1).trim().split(",");
            JSONArray columnsList = new JSONArray();
            for (String column : columns) {
                columnsList.put(column.trim());
            }
            queryJSON.put("columns", columnsList);
            queryJSON.put("table", matcher1.group(3).trim());

            //WHERE
            JSONObject whereJSON = new JSONObject();
            whereJSON.put("value1", matcher1.group(4).trim());
            whereJSON.put("operator", matcher1.group(5).trim());
            whereJSON.put("value2", matcher1.group(6).trim());
            queryJSON.put("where", whereJSON);
//		  log.info(queryJSON);
        } else {
            System.out.println("error");
//		  log.info("SQL Query parse error");
        }

        return queryJSON;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String sqlQuery = "select occupation from users where gender = f";
        JSONObject queryJSON = parseSQL(sqlQuery.toLowerCase());
        System.out.println(queryJSON.length());

        conf.set("queryJSONString", queryJSON.toString());


        Job job = Job.getInstance(conf, "Sql query Executor");
        job.setJarByClass(HadoopExecutor.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputDir = "D:\\IntelliJ\\Cloud_Assignment\\data\\input";
        String outputDir = "D:\\IntelliJ\\Cloud_Assignment\\data\\output";
        if (queryJSON.keySet().contains("joinType")) {
            String table1Name = queryJSON.getJSONObject("table").getString("table1");
            String table2Name = queryJSON.getJSONObject("table").getString("table2");

            MultipleInputs.addInputPath(job, new Path(inputDir + "\\" + table1Name + ".csv"), TextInputFormat.class, MultiQueryMapper1.class);
            MultipleInputs.addInputPath(job, new Path(inputDir + "\\" + table2Name + ".csv"), TextInputFormat.class, MultiQueryMapper2.class);
        } else {
            String tableName = queryJSON.getString("table");
            FileInputFormat.addInputPath(job, new Path(inputDir + "\\" + tableName + ".csv"));
            job.setMapperClass(QueryMapper.class);
        }
        job.setReducerClass(QueryReducer.class);
        //FileInputFormat.addInputPath(job, new Path("C:\\Users\\OMEN\\Desktop\\courses\\cloud_computing\\A1\\CCfinal\\data\\input\\users.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        long start = new Date().getTime();
        boolean status = job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println("Hadoop Execution Time " + (end - start) + "milliseconds");
    }
}