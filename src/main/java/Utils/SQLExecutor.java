package Utils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import cloud_assignment.one.utils.tables.*;

public class SQLExecutor {
	public static String[] jsonArrayToStringArray(JSONArray jsonArray) {
		int arraySize = jsonArray.length();
		String[] stringArray = new String[arraySize];

		for(int i=0; i<arraySize; i++) {
			stringArray[i] = (String) jsonArray.get(i);
		}

		return stringArray;
	}
//  static final Logger log = Logger.getLogger(SQLExecutor.class.getName());

//  public static class QueryMapper
//       extends Mapper<LongWritable, Text, Text, Text>{
//	private static JSONObject queryJSON;
//	public void setup(Context context) {
//		Configuration conf = context.getConfiguration();
//        setQueryJSON(new JSONObject(conf.get("queryJSONString")));
//    }
//    private Text outputRow = new Text();
//
//    public void map(LongWritable key, Text value, Context context
//                    ) throws IOException, InterruptedException {
//    	if(key.get() == 0) {
//    		return;
//    	}
//    	String tableName = queryJSON.getString("table");
//    	Table row = TableFactory.getTable(tableName, value.toString());
//    	if(row == null)
//			return;
////    	System.out.println("row  is not null");
//    	JSONObject whereJSON = queryJSON.getJSONObject("where");
////    	System.out.println(whereJSON);
//    	if(row.checkColumnValue(whereJSON.getString("field"), whereJSON.getString("value")) == false) {
//    		return;
//    	}
////    	System.out.println("row matches column value");
//    	outputRow.set(row.groupByString(jsonArrayToStringArray(queryJSON.getJSONArray("groupByColumns"))));
//    	context.write(outputRow, value);
//    }
//
//	public static JSONObject getQueryJSON() {
//		return queryJSON;
//	}
//
//	public static void setQueryJSON(JSONObject queryJSON) {
//		QueryMapper.queryJSON = queryJSON;
//	}
//  }

//  public static class QueryReducer
//       extends Reducer<Text,Text,Text,Text> {
//	  private static JSONObject queryJSON;
//		public void setup(Context context) {
//			Configuration conf = context.getConfiguration();
////			System.out.println("Inside Reducer Setup ========> ");
////			System.out.println(conf.get("queryJSONString"));
//	        setQueryJSON(new JSONObject(conf.get("queryJSONString")));
////	        System.out.println("Inside reducer Setup =====> ".concat(queryJSON.toString()));
//	    }
//	    private Text outputRowText = new Text();
//
//    public void reduce(Text key, Iterable<Text> values,
//                       Context context
//                       ) throws IOException, InterruptedException {
////    	System.out.println(key.toString());
//    	String tableName = queryJSON.getString("table");
//    	ArrayList<Table> arr = new ArrayList<Table>();
//    	for(Text v : values) {
////    		System.out.println("value in reducer =====> ");
////    		System.out.println(v.toString());
//    		Table row = TableFactory.getTable(tableName, v.toString());
////    		System.out.println("inside reducer === ");
////    		System.out.println(v.toString());
//    		if(row == null)
//    			continue;
//    		arr.add(row);
//    	}
//    	System.out.println(arr.size());
//    	if(arr.size() == 0)
//    		return;
//    	JSONObject havingJSON = queryJSON.getJSONObject("having");
//    	JSONObject aggregateJSON = queryJSON.getJSONObject("aggregate");
//    	JSONArray columnsArray = queryJSON.getJSONArray("columns");
//    	String outputRow = new String("");
//    	int i = 0;
////    	System.out.println("arr =====> ");
////    	System.out.println(arr.get(0));
////    	System.out.println(havingJSON);
//    	String aggr = arr.get(0).getAggregate(aggregateJSON.getString("function"), aggregateJSON.getString("field"), arr).toString();
//    	if(arr.get(0).compareAggregate(havingJSON.getString("column"), havingJSON.getString("function"),
//    			havingJSON.getString("operator"), havingJSON.getString("value"), arr) == false) {
//    		System.out.println("aggregate not matched================");
//    		return;
//    	}
//		for(i = 0; i < columnsArray.length(); i++) {
////			System.out.println("columns array ===>");
////			System.out.println(arr.get(0));
//			outputRow = outputRow.concat(arr.get(0).getColumnValue(columnsArray.getString(i)).toString()).concat(",");
//		}
//		outputRow = outputRow.concat(aggr);
//		System.out.println("printing oputput row === ");
//		System.out.println(outputRow);
//		outputRowText.set(outputRow);
//		System.out.println("Writing out from reducer ==============");
//		context.write(key, outputRowText);
//    }
//    public static JSONObject getQueryJSON() {
//		return queryJSON;
//	}
//
//	public static void setQueryJSON(JSONObject queryJSON) {
//		QueryReducer.queryJSON = queryJSON;
//	}
//  }

	public static JSONObject parseSQL(String query) {

		//ignore semicolon at the end;
		if(query.charAt(query.length()-1) == ';'){
			query = query.substring(0, query.length() - 1);
		}
		//remove all the inverted commas
		query = query.replaceAll("[']"," ");

		Pattern pattern1 = Pattern.compile("select (.+(,.+)*) from(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+)");
		Pattern pattern2 = Pattern.compile("select (.+(,.+)*) from(.+) ((natural)? ((left|right) outer )|inner )?join (.+) on (.+)=(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+)");
		Pattern pattern3 = Pattern.compile("select (.+(,.+)*) from(.+) where(.+) (=|>|<|>=|<=|<>|like|in) (.+) group by (.+) having (sum|count|max|min)\\((.+)\\) (>=|<=|==|!=|>|<) (.+)");
		Matcher matcher1 = pattern1.matcher(query);
		Matcher matcher2 = pattern2.matcher(query);
		Matcher matcher3 = pattern3.matcher(query);
		JSONObject queryJSON = new JSONObject();

		if(matcher2.matches()){
			String[] columns = matcher2.group(1).trim().split(",");
			JSONArray columnsList = new JSONArray();
			for(String column : columns) {
				columnsList.put(column.trim());
			}
			queryJSON.put("columns", columnsList);

//		  JOIN part
			String joinType  = query.split("join")[0].split("from")[1];
			String [] arr = joinType.split(" ", 3);
			if (arr[2].equals("")) {
				arr[2] = "inner";
			}
			queryJSON.put("joinType", arr[2].split(" ",2)[0]);

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
		}
		else if(matcher3.matches()){
			//SELECT
			String[] columns = matcher3.group(1).trim().split(",");
			JSONArray columnsList = new JSONArray();
			for(String column : columns) {
				if(column.trim().contains("(")){
					JSONObject aggregateJSON = new JSONObject();
					aggregateJSON.put("function",column.split("\\(")[0].trim());
					String temp = column.split("\\(")[1].trim();
					temp = temp.substring(0, temp.length() - 1);
					aggregateJSON.put("column",temp);
					columnsList.put(aggregateJSON);
				} else{
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
			for(String groupByColumn : groupByColumns) {
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
		}
		else if(matcher1.matches()) {
			//SELECT
			String[] columns = matcher1.group(1).trim().split(",");
			JSONArray columnsList = new JSONArray();
			for(String column : columns) {
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
		}
		else {
			System.out.println("error");
//		  log.info("SQL Query parse error");
		}

		return queryJSON;
	}

	public static void main(String[] args) throws Exception {
//    Configuration conf = new Configuration();
		String sqlQuery = "select userid, age from users outer join rating on users.userid = rating.userid where gender = 'M';";
		JSONObject queryJSON = parseSQL(sqlQuery.toLowerCase());
		System.out.println(queryJSON);
//    conf.set("queryJSONString", queryJSON.toString());
//
//    Job job = Job.getInstance(conf, "word count");
//    job.setJarByClass(SQLExecutor.class);
//    job.setMapperClass(QueryMapper.class);
//    job.setReducerClass(QueryReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(Text.class);
//    FileInputFormat.addInputPath(job, new Path("C:\\Users\\OMEN\\Desktop\\courses\\cloud computing\\A1\\1\\data\\input\\category.csv"));
//    FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\OMEN\\Desktop\\courses\\cloud computing\\A1\\1\\data\\output"));
//
//    long start = new Date().getTime();
//    boolean status = job.waitForCompletion(true);
//    long end = new Date().getTime();
//    System.out.println("Hadoop Execution Time "+(end-start) + "milliseconds");
//    System.out.println("Mapper input -> <LongWritable, Text> (offset, line of file)");
//    System.out.println("Mapper output -> <Text, Text> (columns in group by separated by _, row satisfying where condition)");
//    System.out.println("Reducer input -> <Text, Text> (columns in group by separated by _, row)");
//    System.out.println("Reducer output -> <Text, Text> (columns in group by separated by _, row containing required columns and satisfying having condition)");


//    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
//
//  public static JSONObject hadoopOutput(String[] args) throws Exception {
//	String sqlQuery = args[2];
//    Configuration conf = new Configuration();
//    JSONObject queryJSON = parseSQL(sqlQuery.toLowerCase());
//    System.out.println(queryJSON);
//    conf.set("queryJSONString", queryJSON.toString());
//
//    Job job = Job.getInstance(conf, "word count");
//    job.setJarByClass(SQLExecutor.class);
//    job.setMapperClass(QueryMapper.class);
//    job.setReducerClass(QueryReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(Text.class);
//    FileInputFormat.addInputPath(job, new Path(args[0].concat("/").concat(queryJSON.getString("table")).concat(".csv")));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//    long start = new Date().getTime();
//    boolean status = job.waitForCompletion(true);
//    long end = new Date().getTime();
////    System.out.println("Hadoop Execution Time "+(end-start) + "milliseconds");
////    System.out.println("Mapper input -> <LongWritable, Text> (offset, line of file)");
////    System.out.println("Mapper output -> <Text, Text> (columns in group by separated by _, row satisfying where condition)");
////    System.out.println("Reducer input -> <Text, Text> (columns in group by separated by _, row)");
////    System.out.println("Reducer output -> <Text, Text> (columns in group by separated by _, row containing required columns and satisfying having condition)");
//
//    JSONObject hadoopSpecifications = new JSONObject();
//    hadoopSpecifications.put("Hadoop Execution Time in ms", new Long(end-start));
//    hadoopSpecifications.put("Mapper input", "<LongWritable, Text> (offset, line of file)");
//    hadoopSpecifications.put("Mapper output", "<Text, Text> (columns in group by separated by _, row satisfying where condition)");
//    hadoopSpecifications.put("Reducer input", "<Text, Text> (columns in group by separated by _, row)");
//    hadoopSpecifications.put("Reducer output", "<Text, Text> (columns in group by separated by _, row containing required columns and satisfying having condition)");
//
//    return hadoopSpecifications;
////	    System.exit(job.waitForCompletion(true) ? 0 : 1);
//  }
}
